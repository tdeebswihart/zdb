const std = @import("std");
const block = @import("block.zig");
const Entry = @import("entry.zig").Entry;

// Storage managers manage access to individual files.
// Each file is carved up into blocks which we treat as a generic heap.
// TODO track occupancy using an occupancy map rather than a list of blocks.
// This manager will never reclaim space.
//
// FILE HEADER
// - u16 block size
// - u16 number of blocks
// - number of blocks bytes for the occupancy map

pub const FileHeader = struct {
    block_size: u16 = 0,
    allocated_blocks: u64 = 0,
};

pub const GetError = error {
    PageDoesntExist,
};

pub fn Manager(comptime T: type) type {
    return struct {
        const PageT = block.Page(T);

        header: FileHeader,
        file: T,
        file_size: u64,
        mem: *std.mem.Allocator,
        blocks: std.ArrayList(*PageT),

        const Self = @This();

        pub fn init(file: T, block_size: u16, mem: *std.mem.Allocator) !*Self {
            var hdr = FileHeader{.block_size = block_size};
            var blocks = std.ArrayList(*PageT).init(mem);
            var file_size: u64 = 0;

            const size = try file.size();
            if (size == 0) {
                // New file, write our initial header
                try file.writeAll(std.mem.asBytes(&hdr));
                file_size = @sizeOf(FileHeader);
            } else {
                file_size = size;
                // read existing header
                try file.seekTo(0);
                _ = try file.read(std.mem.asBytes(&hdr));
                std.debug.assert(hdr.block_size == block_size);

                try blocks.resize(hdr.allocated_blocks);

                var offset: u64 = @sizeOf(FileHeader);
                var i: usize = 0;
                const sz = @sizeOf(FileHeader) + block_size * hdr.allocated_blocks;
                while (offset < sz) : (offset += block_size) {
                    const b= try PageT.init(file, offset, block_size, mem);
                    blocks.items[i] = b;
                    i += 1;
                }
            }
            const mgr = try mem.create(Self);
            mgr.header = hdr;
            mgr.file = file;
            mgr.file_size = file_size;
            mgr.mem = mem;
            mgr.blocks = blocks;

            return mgr;
        }

        pub fn deinit(self: *Self) !void {
            std.debug.assert(self.header.allocated_blocks == self.blocks.items.len);
            try self.file.seekTo(0);
            try self.file.writeAll(std.mem.asBytes(&self.header));
            for (self.blocks.items) |b| {
                try b.deinit();
            }
            self.blocks.deinit();
            self.mem.destroy(self);
        }

        pub fn put(self: *Self, record: []const u8) !Entry {
            var i: u64 = 0;
            var end = self.blocks.items.len;
            while (i < end) : (i += 1) {
                const b = self.blocks.items[i];
                if (b.can_contain(@intCast(u16, record.len))) {
                    break;
                }
            }
            const bl = blk: {
                if (i == end) {
                    // No blocks found, alloc a new one
                    const b = try PageT.init(self.file, self.file_size, self.header.block_size, self.mem);
                    self.file_size += self.header.block_size;
                    try self.file.extend(self.file_size);
                    try self.blocks.append(b);
                    self.header.allocated_blocks += 1;
                    break :blk b;
                }
                break :blk self.blocks.items[i];
            };
            // TODO if block is out of space update occupancy and make a new one
            var entry = try bl.put(record);
            entry.block = @intCast(u16, i);
            return entry;
        }

        pub fn get(self: *Self, entry: Entry) ![]const u8 {
            if (entry.block > self.blocks.items.len) {
                return GetError.PageDoesntExist;
            }
            const b = self.blocks.items[entry.block];
            return b.get(entry.slot);
        }

        pub fn update(self: *Self, oldEntry: Entry, record: []const u8) !void {
            const data = try b.get(entry);
            if (data.len != record.len) {
                return UpdateEError.InvalidLength;
            }
            const bl = self.blocks.items[oldEntry.block];
            try bl.delete(oldEntry.slot);
            var entry = try bl.put(record);
            entry.block = entry.block;
            return entry;
        }

        pub fn delete(self: *Self, entry: Entry) !void {
            @panic("FIXME: implement delete");
        }
    };
}
