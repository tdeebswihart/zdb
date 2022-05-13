const std = @import("std");
const FileManager = @import("file.zig").Manager;
const PAGE_SIZE = @import("config.zig").PAGE_SIZE;
const page = @import("page.zig");
const Latch = @import("libdb").sync.Latch;
const Crc32 = std.hash.Crc32;
const Entry = @import("entry.zig").Entry;
const assert = std.debug.assert;

pub const Slot = struct {
    // The offset of this Record within its block
    offset: u16,
    // Entries can be at most 1 block in size
    // A size of -1 means "deleted"
    size: u16,
};

pub const Error = error{
    ChecksumMismatch,
    InvalidPage,
    OutOfSpace,
};

pub const GetError = Error || error{
    RecordDoesntExist,
    RecordDeleted,
};

// Each block is of fixed length and acts as a bump-up
// allocator for contained records.
// A periodic compaction (or vacuum) process should clean up
// deleted blocks so there are no gaps in the file.
pub const TuplePage = packed struct {
    const slotSpace = PAGE_SIZE - (2 * @sizeOf(u32) + @sizeOf(page.Header));
    header: page.Header,
    remainingSpace: u16 = 0,
    freeSpace: u16 = 0,
    slotsInUse: u32 = 0,
    slots: [slotSpace]u8,

    const Self = @This();

    pub fn init(p: *page.ControlBlock) *Self {
        return @ptrCast(*Self, @alignCast(@alignOf(Self), p.buffer[0..]));
    }

    pub fn new(p: *page.ControlBlock) *Self {
        var self = Self.init(p);
        self.remainingSpace = slotSpace;
        self.freeSpace = slotSpace;
        self.slotsInUse = 0;
        p.dirty = true;
        return self;
    }

    pub fn can_contain(self: *Self, amount: u16) bool {
        return self.remainingSpace > (amount + @sizeOf(Slot));
    }

    fn findSlot(self: *Self, index: u16) GetError!*align(1) Slot {
        if (index >= self.slotsInUse) {
            return GetError.RecordDoesntExist;
        }
        const start = index * @sizeOf(Slot);
        const mem = self.slots[start .. start + @sizeOf(Slot)];
        // I don't see why this is necessary but the compiler demands it
        return std.mem.bytesAsValue(Slot, mem[0..@sizeOf(Slot)]);
    }
};

pub const Readable = struct {
    page: *page.ControlBlock,
    hold: Latch.Hold,
    inner: ?*TuplePage,

    pub fn init(p: *page.ControlBlock) !@This() {
        var hold = p.latch.shared();
        return Readable{ .inner = TuplePage.init(p), .page = p, .hold = hold };
    }

    pub fn deinit(self: *@This()) void {
        assert(self.inner != null);
        self.hold.release();
        self.inner = null;
    }

    pub fn get(self: @This(), slotIdx: u16) GetError![]const u8 {
        const inner = self.inner orelse return GetError.InvalidPage;
        const slot = try inner.findSlot(slotIdx);
        if (slot.size == -1) {
            return GetError.RecordDeleted;
        }
        return inner.slots[slot.offset .. slot.offset + slot.size];
    }
};

pub const Writable = struct {
    inner: ?*TuplePage,
    hold: Latch.Hold,
    page: *page.ControlBlock,

    pub fn init(p: *page.ControlBlock) !@This() {
        var hold = p.latch.exclusive();
        errdefer hold.release();
        return Writable{ .inner = TuplePage.init(p), .page = p, .hold = hold };
    }

    pub fn deinit(self: *@This()) void {
        self.inner = null;
        self.hold.release();
    }

    pub fn get(self: @This(), slotIdx: u16) GetError![]const u8 {
        const inner = self.inner orelse return GetError.InvalidPage;
        const slot = try inner.findSlot(slotIdx);
        if (slot.size == -1) {
            return GetError.RecordDeleted;
        }
        return inner.slots[slot.offset .. slot.offset + slot.size];
    }

    pub fn put(self: @This(), record: []const u8) Error!Entry {
        const inner = self.inner orelse return Error.InvalidPage;
        const bytesNecessary = @intCast(u16, record.len) + @sizeOf(Slot);
        if (inner.remainingSpace < bytesNecessary) {
            return Error.OutOfSpace;
        }

        const offset = inner.freeSpace;
        std.mem.copy(u8, inner.slots[offset - record.len .. offset], record);
        inner.freeSpace -= @intCast(u16, record.len);
        inner.remainingSpace -= bytesNecessary;

        const recordNum = inner.slotsInUse;
        const slot = Slot{
            .offset = inner.freeSpace,
            .size = @intCast(u16, record.len),
        };
        var slotStart = inner.slotsInUse * @sizeOf(Slot);
        var slotMem = inner.slots[slotStart .. slotStart + @sizeOf(Slot)];
        std.mem.copy(u8, slotMem, std.mem.asBytes(&slot));
        inner.slotsInUse += 1;

        self.page.dirty = true;

        return Entry{
            .page = self.page.id(),
            .slot = @intCast(u16, recordNum),
        };
    }

    pub fn delete(self: *@This(), slotIdx: u16) Error!void {
        const inner = self.inner orelse return Error.InvalidPage;
        const slot = try inner.findSlot(slotIdx);
        if (slot.size == -1) {
            return Error.RecordDeleted;
        }
        slot.size = -1;
        self.page.dirty = true;
    }
};
