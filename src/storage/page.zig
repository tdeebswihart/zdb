const std = @import("std");
const Entry = @import("entry.zig").Entry;
const Store = @import("file.zig").Store;
const Crc32 = std.hash.Crc32;
const Latch = @import("libdb").sync.Latch;
const LatchHold = @import("libdb").sync.LatchHold;

pub const Slot = struct {
    // The offset of this Record within its block
    offset: u16,
    // Entries can be at most 1 block in size
    // A size of -1 means "deleted"
    size: u16,
};

pub const PageError = error{
    OutOfSpace,
    ChecksumMismatch,
};

const Magic: u32 = 0xD3ADB33F;

pub const Header = struct {
    magic: u32 = 0,
    remainingSpace: u16 = 0,
    freeSpace: u16 = 0,
    slotsInUse: u32 = 0,
    crc32: u32 = 0,

    pub fn init(self: *align(1) @This(), pageSz: u16) void {
        self.magic = Magic;
        self.remainingSpace = pageSz;
        self.freeSpace = pageSz;
        self.slotsInUse = 0;
        self.crc32 = 0;
    }
};

pub const GetError = error{
    RecordDoesntExist,
    RecordDeleted,
};

pub const Held = struct {
    page: *Page,

    pub fn deinit(self: @This()) void {
        self.page.unpin();
        self.page = null;
    }
};

// Each block is of fixed length and acts as a bump-up
// allocator for contained records.
// A periodic compaction (or vacuum) process should clean up
// deleted blocks so there are no gaps in the file.
pub const Page = struct {
    live: bool = false,
    file: *Store,
    id: u64 = 0,
    offset: u64 = 0,
    size: u16 = 0,
    lastAccess: usize = 0,
    pins: u64 = 0,
    dirty: bool = false,
    header: *align(1) Header,
    buffer: []u8 = &[_]u8{},
    latch: *Latch,
    mem: *std.mem.Allocator,

    const Self = @This();

    pub fn init(self: *Self, mem: *std.mem.Allocator, buf: []u8) !void {
        self.mem = mem;
        self.latch = try Latch.init(self.mem);
        self.buffer = buf;
        self.live = false;
    }

    pub fn pinned(self: *Self) bool {
        return @atomicLoad(u64, &self.pins, .Acquire) > 0;
    }

    pub fn pin(self: *Self) Pin {
        _ = @atomicRmw(u64, &self.pins, .Add, 1, .Acquire);
        return Pin{
            .page = self,
        };
    }

    pub fn reinit(self: *Self, file: *Store, id: u64, offset: u64, size: u16) !void {
        const sz = try file.size();
        if (offset > sz) {
            try file.extend(offset + size);
        }
        try file.seekTo(offset);

        _ = try file.readAll(std.mem.sliceAsBytes(self.buffer));
        var hdr = std.mem.bytesAsValue(Header, self.buffer[0..@sizeOf(Header)]);
        if (hdr.magic != Magic) {
            hdr.init(size);
        } else {
            const expected = Crc32.hash(self.buffer[@sizeOf(Header)..]);
            if (hdr.crc32 != expected) {
                return PageError.ChecksumMismatch;
            }
        }

        self.id = id;
        self.file = file;
        self.offset = offset;
        self.size = size;
        self.header = hdr;
        self.dirty = false;
        self.live = true;
    }

    pub fn writeback(self: *Self) !void {
        if (self.live and self.dirty) {
            self.header.crc32 = Crc32.hash(self.buffer[@sizeOf(Header)..]);
            try self.file.seekTo(self.offset);
            _ = try self.file.writeAll(self.buffer);
        }
    }

    pub fn deinit(self: *Self) !void {
        try self.writeback();
        self.mem.destroy(self.latch);
    }

    pub fn can_contain(self: *Self, amount: u16) bool {
        return self.header.remainingSpace > (amount + @sizeOf(Slot));
    }

    fn findSlot(self: *Self, index: u16) GetError!*align(1) Slot {
        if (index >= self.header.slotsInUse) {
            return GetError.RecordDoesntExist;
        }
        const start = @sizeOf(Header) + index * @sizeOf(Slot);
        const mem = self.buffer[start .. start + @sizeOf(Slot)];
        // I don't see why this is necessary but the compiler demands it
        return std.mem.bytesAsValue(Slot, mem[0..@sizeOf(Slot)]);
    }
};

pub const Pin = struct {
    page: *Page,

    pub fn unpin(self: *@This()) void {
        _ = @atomicRmw(u64, &self.page.pins, .Sub, 1, .Release);
        self.page = undefined;
    }

    pub fn shared(self: @This()) SharedPage {
        return SharedPage{
            .page = self.page,
            .hold = self.page.latch.shared(),
        };
    }

    pub fn exclusive(self: @This()) ExclusivePage {
        return ExclusivePage{
            .page = self.page,
            .hold = self.page.latch.exclusive(),
        };
    }
};

pub const SharedPage = struct {
    page: *Page,
    hold: LatchHold,

    const Self = @This();

    pub fn deinit(self: *Self) void {
        self.hold.release();
    }

    pub fn get(self: Self, slotIdx: u16) GetError![]const u8 {
        const slot = try self.page.findSlot(slotIdx);
        if (slot.size == -1) {
            return GetError.RecordDeleted;
        }
        return self.page.buffer[slot.offset .. slot.offset + slot.size];
    }
};

pub const ExclusivePage = struct {
    page: *Page,
    hold: LatchHold,

    const Self = @This();

    pub fn deinit(self: *Self) void {
        self.hold.release();
    }

    pub fn get(self: Self, slotIdx: u16) GetError![]const u8 {
        const slot = try self.page.findSlot(slotIdx);
        if (slot.size == -1) {
            return GetError.RecordDeleted;
        }
        return self.page.buffer[slot.offset .. slot.offset + slot.size];
    }

    pub fn put(self: Self, record: []const u8) !Entry {
        const bytesNecessary = @intCast(u16, record.len) + @sizeOf(Slot);
        var page = self.page;
        if (page.header.remainingSpace < bytesNecessary) {
            return PageError.OutOfSpace;
        }

        const offset = page.header.freeSpace;
        std.mem.copy(u8, page.buffer[offset - record.len .. offset], record);
        page.header.freeSpace -= @intCast(u16, record.len);
        page.header.remainingSpace -= bytesNecessary;

        const recordNum = page.header.slotsInUse;
        const slot = Slot{
            .offset = page.header.freeSpace,
            .size = @intCast(u16, record.len),
        };
        var slotStart = @sizeOf(Header) + page.header.slotsInUse * @sizeOf(Slot);
        var slotMem = page.buffer[slotStart .. slotStart + @sizeOf(Slot)];
        std.mem.copy(u8, slotMem, std.mem.asBytes(&slot));
        page.header.slotsInUse += 1;
        page.dirty = true;

        return Entry{
            .page = page.id,
            .slot = @intCast(u16, recordNum),
        };
    }

    pub fn delete(self: *Self, slotIdx: u16) GetError!void {
        var page = self.page;
        const slot = try page.findSlot(slotIdx);
        if (slot.size == -1) {
            return GetError.RecordDeleted;
        }
        slot.size = -1;
        page.dirty = true;
    }
};
