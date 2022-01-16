const std = @import("std");
const FileManager = @import("file.zig").Manager;
const PAGE_SIZE = @import("config.zig").PAGE_SIZE;
const Page = @import("page.zig").Page;
const Latch = @import("libdb").sync.Latch;
const Crc32 = std.hash.Crc32;
const Entry = @import("entry.zig").Entry;
const assert = std.debug.assert;

const MAGIC: u32 = 0xD3ADB33F;

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

const Header = struct {
    magic: u32 = 0,
    remainingSpace: u16 = 0,
    freeSpace: u16 = 0,
    slotsInUse: u32 = 0,
};

// Each block is of fixed length and acts as a bump-up
// allocator for contained records.
// A periodic compaction (or vacuum) process should clean up
// deleted blocks so there are no gaps in the file.
pub const TuplePage = struct {
    const slotSpace = PAGE_SIZE - (@sizeOf(u32) + @sizeOf(Header));

    crc32: u32 = 0,
    header: Header,
    slots: [slotSpace]u8,

    const Self = @This();

    pub fn init(page: *Page) Error!*Self {
        var self = @ptrCast(*Self, @alignCast(@alignOf(Self), page.buffer[0..]));
        if (self.header.magic != MAGIC) {
            self.header.magic = MAGIC;
            self.header.remainingSpace = slotSpace;
            self.header.freeSpace = slotSpace;
            self.header.slotsInUse = 0;
            self.crc32 = self.hash();
            page.dirty = true;
        } else {
            const expected = self.hash();
            if (self.crc32 != expected) {
                return Error.ChecksumMismatch;
            }
        }
        return self;
    }

    pub fn hash(self: *Self) u32 {
        return Crc32.hash(std.mem.asBytes(self)[@offsetOf(Self, "crc32") + @sizeOf(u32) ..]);
    }

    pub fn can_contain(self: *Self, amount: u16) bool {
        return self.header.remainingSpace > (amount + @sizeOf(Slot));
    }

    fn findSlot(self: *Self, index: u16) GetError!*align(1) Slot {
        if (index >= self.header.slotsInUse) {
            return GetError.RecordDoesntExist;
        }
        const start = index * @sizeOf(Slot);
        const mem = self.slots[start .. start + @sizeOf(Slot)];
        // I don't see why this is necessary but the compiler demands it
        return std.mem.bytesAsValue(Slot, mem[0..@sizeOf(Slot)]);
    }
};

pub const Readable = struct {
    page: *Page,
    hold: Latch.Hold,
    inner: ?*TuplePage,

    pub fn init(page: *Page) !@This() {
        var hold = page.latch.shared();
        errdefer hold.release();
        return Readable{ .inner = try TuplePage.init(page), .page = page, .hold = hold };
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
    page: *Page,

    pub fn init(page: *Page) !@This() {
        var hold = page.latch.exclusive();
        errdefer hold.release();
        return Writable{ .inner = try TuplePage.init(page), .page = page, .hold = hold };
    }

    pub fn deinit(self: *@This()) void {
        var inner = self.inner orelse {
            assert(false);
            return;
        };
        inner.crc32 = inner.hash();
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
        if (inner.header.remainingSpace < bytesNecessary) {
            return Error.OutOfSpace;
        }

        const offset = inner.header.freeSpace;
        std.mem.copy(u8, inner.slots[offset - record.len .. offset], record);
        inner.header.freeSpace -= @intCast(u16, record.len);
        inner.header.remainingSpace -= bytesNecessary;

        const recordNum = inner.header.slotsInUse;
        const slot = Slot{
            .offset = inner.header.freeSpace,
            .size = @intCast(u16, record.len),
        };
        var slotStart = inner.header.slotsInUse * @sizeOf(Slot);
        var slotMem = inner.slots[slotStart .. slotStart + @sizeOf(Slot)];
        std.mem.copy(u8, slotMem, std.mem.asBytes(&slot));
        inner.header.slotsInUse += 1;

        self.page.dirty = true;

        return Entry{
            .page = self.page.id,
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
