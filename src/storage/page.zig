const std = @import("std");
const Entry = @import("entry.zig").Entry;
const Store = @import("file.zig").Store;

pub const Slot = struct {
    // The offset of this Record within its block
    offset: u16,
    // Entries can be at most 1 block in size
    // A size of -1 means "deleted"
    size: u16,
};

pub const PageError = error {
    OutOfSpace,
};

pub const Header = struct {
    remaining_space: u16 = 0,
    free_space: u16 = 0,
    slots_inuse: u32 = 0,

    pub fn invalid(self: Header) bool {
        return (self.slots_inuse == 0) and (self.free_space == 0);
    }
};

pub const GetError = error {
    RecordDoesntExist,
    RecordDeleted,
};

// Each block is of fixed length and acts as a bump-up
// allocator for contained records.
// A periodic compaction (or vacuum) process should clean up
// deleted blocks so there are no gaps in the file.
pub const Page = struct {
    file: *Store,
    id: u64 = 0,
    offset: u64 = 0,
    size: u16 = 0,
    lastAccess: usize = 0,
    dirty: bool = false,
    header: *align(1) Header = null,
    buffer: []u8 = .{},

    const Self = @This();

    pub fn reinit(self: *Self, file: *Store, id: u64, offset: u64, size: u16) !void {
        try file.seekTo(offset);

        self.dirty = false;
        _ = try file.readAll(std.mem.sliceAsBytes(self.buffer));
        // FIXME get header from buf
        var hdr = std.mem.bytesAsValue(Header, self.buffer[0..@sizeOf(Header)]);
        if (hdr.invalid()) {
            hdr.free_space = size;
            hdr.remaining_space = size - @sizeOf(Header);
        }

        self.id = id;
        self.file = file;
        self.offset = offset;
        self.size = size;
        self.header = hdr;
    }

    pub fn flush(self: *Self) !void {
        if (self.dirty) {
            try self.file.seekTo(self.offset);
            _ = try self.file.writeAll(self.buffer);
        }
    }

    pub fn deinit(self: *Self) !void {
        try self.flush();
    }

    pub fn can_contain(self: *Self, amount: u16) bool {
        return self.header.remaining_space > (amount + @sizeOf(Slot));
    }

    fn findSlot(self: *Self, index: u16) GetError!*align(1) Slot {
        if (index > self.header.slots_inuse) {
            return GetError.RecordDoesntExist;
        }
        const start = @sizeOf(Header) + index*@sizeOf(Slot);
        const mem = self.buffer[start..start+@sizeOf(Slot)];
        // I don't see why this is necessary but the compile demands it
        return std.mem.bytesAsValue(Slot, mem[0..@sizeOf(Slot)]);
    }

    pub fn put(self: *Self, record: []const u8) !Entry {
        const bytesNecessary = @intCast(u16, record.len) + @sizeOf(Slot);
        if (self.header.remaining_space < bytesNecessary) {
            return PageError.OutOfSpace;
        }

        const offset = self.offset + self.header.free_space;
        std.mem.copy(u8, self.buffer[offset..offset + record.len], record);

        const record_num = self.header.slots_inuse;
        const slot = Slot{
            .offset = self.header.free_space,
            .size = @intCast(u16, record.len),
        };
        var slotStart = @sizeOf(Header) + self.header.slots_inuse*@sizeOf(Slot);
        var slotMem = self.buffer[slotStart..slotStart+@sizeOf(Slot)];
        std.mem.copy(u8, slotMem, std.mem.asBytes(&slot));
        self.header.slots_inuse += 1;
        self.header.free_space -= bytesNecessary;
        self.dirty = true;

        return Entry{
            .block = self.id,
            .slot = @intCast(u16, record_num),
        };
    }

    pub fn delete(self: *Self, slotIdx: u16) GetError!void {
        const slot = try self.findSlot(slotIdx);
        if (slot.size == -1) {
            return GetError.RecordDeleted;
        }
        slot.size = -1;
        self.dirty = true;
    }

    pub fn get(self: *Self, slotIdx: u16) GetError![]const u8 {
        const slot = try self.findSlot(slotIdx);
        if (slot.size == -1) {
            return GetError.RecordDeleted;
        }
        return self.buffer[slot.offset..slot.offset+slot.size];
    }
};
