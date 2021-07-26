const std = @import("std");
const fs = std.fs;

const Record = struct {
    // The offset of this Record in its block
    offset: u16,
    // Entries can be at most 1 block in size
    // A size of -1 means "deleted"
    size: u16,
};

const BlockError = error {
    OutOfSpace,
};

const BlockHeader = struct {
    remaining_space: u16 = 0,
    free_space: u16 = 0,
    active_records: u32 = 0,

    pub fn invalid(self: BlockHeader) bool {
        return (self.active_records == 0) and (self.free_space == 0);
    }
};

// Each block is of fixed length and acts as a bump-up
// allocator for contained records.
// A periodic compaction (or vacuum) process should clean up
// deleted blocks so there are no gaps in the file.
const Block = struct {
    file: fs.File,
    offset: u64,
    size: u16,
    header: BlockHeader,
    records: std.ArrayList(Record),
    mem: *std.mem.Allocator,

    pub fn init(file: fs.File, offset: u64, size: u16, mem: *std.mem.Allocator) !*Block {
        try file.seekTo(offset);

        var hdr = BlockHeader{};
        _ = try file.read(std.mem.asBytes(&hdr));
        const records = blk: {
            if (hdr.invalid()) {
                hdr.free_space = size;
                hdr.remaining_space = size - @sizeOf(BlockHeader);
                break :blk std.ArrayList(Record).init(mem);
            }
            const recs = try std.ArrayList(Record).initCapacity(mem, hdr.active_records);
            _ = try file.readAll(std.mem.sliceAsBytes(recs.items));
            break :blk recs;
        };

        var b = try mem.create(Block);
        b.file = file;
        b.offset = offset;
        b.size = size;
        b.header = hdr;
        b.records = records;
        b.mem = mem;
        return b;
    }

    pub fn deinit(self: *Block) !void {
        try self.file.seekTo(self.offset);
        _ = try self.file.writeAll(std.mem.asBytes(&self.header));
        _ = try self.file.writeAll(std.mem.sliceAsBytes(self.records.items));
        self.records.deinit();
        self.mem.destroy(self);
    }

    pub fn can_contain(self: *Block, amount: u16) bool {
        return self.header.remaining_space > (amount + @sizeOf(Record));
    }

    pub fn put(self: *Block, record: []const u8) !Entry {
        const sz = @intCast(u16, record.len) + @sizeOf(Record);
        if (self.header.remaining_space < sz) {
            return BlockError.OutOfSpace;
        }

        self.header.free_space -= @intCast(u16, record.len);
        const offset = self.offset + self.header.free_space;

        try self.file.seekTo(offset);
        try self.file.writeAll(record);

        const record_num = self.records.items.len;
        try self.records.append(Record{
            .offset = self.header.free_space,
            .size = @intCast(u16, record.len),
        });
        self.header.remaining_space -= sz;

        return Entry{
            .record = @intCast(u16, record_num),
        };
    }
};

// Storage managers manage access to individual files.
// Each file is carved up into blocks which we treat as a generic heap.
// TODO track occupancy using an occupancy map, not a pointer to the next block with space.
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

pub const Manager = struct {
    header: FileHeader,
    file: fs.File,
    file_size: u64,
    mem: *std.mem.Allocator,
    blocks: std.ArrayList(*Block),

    pub fn init(file: std.fs.File, block_size: u16, mem: *std.mem.Allocator) !*Manager {
        var hdr = FileHeader{.block_size = block_size};
        var blocks = std.ArrayList(*Block).init(mem);
        var file_size: u64 = 0;

        const stat = try file.stat();
        if (stat.size == 0) {
            // New file, write our initial header
            try file.writeAll(std.mem.asBytes(&hdr));
            file_size = @sizeOf(FileHeader);
        } else {
            file_size = stat.size;
            // read existing header
            try file.seekTo(0);
            _ = try file.read(std.mem.asBytes(&hdr));
            std.debug.assert(hdr.block_size == block_size);

            try blocks.resize(hdr.allocated_blocks);

            var offset: u64 = @sizeOf(FileHeader);
            while (offset < file_size) : (offset += block_size) {
                const block = try Block.init(file, offset, block_size, mem);
                blocks.appendAssumeCapacity(block);
            }
        }
        const mgr = try mem.create(Manager);
        mgr.header = hdr;
        mgr.file = file;
        mgr.file_size = file_size;
        mgr.mem = mem;
        mgr.blocks = blocks;

        return mgr;
    }

    pub fn deinit(self: *Manager) !void {
        try self.file.seekTo(0);
        try self.file.writeAll(std.mem.asBytes(&self.header));
        for (self.blocks.items) |block| {
            try block.deinit();
        }
        self.blocks.deinit();
        self.file.close();
        self.mem.destroy(self);
    }

    pub fn put(self: *Manager, record: []const u8) !Entry {
        var i: u64 = 0;
        var end = self.blocks.items.len;
        while (i < end) : (i += 1) {
            if (self.blocks.items[i].can_contain(@intCast(u16, record.len))) {
                break;
            }
        }
        const block = blk: {
            if (i == end) {
                // No blocks found, alloc a new one
                const b = try Block.init(self.file, self.file_size, self.header.block_size, self.mem);
                self.file_size += self.header.block_size;
                try self.file.setEndPos(self.file_size);
                try self.blocks.append(b);
                break :blk b;
            }
            break :blk self.blocks.items[i];
        };
        // TODO if block is out of space update occupancy and make a new one
        var entry = try block.put(record);
        entry.block = @intCast(u16, i);
        return entry;
    }

    pub fn get(self: *Manager, entry: Entry) !?[]const u8 {
        @panic("FIXME: implement get");
    }

    pub fn delete(self: *Manager, entry: Entry) !void {
        @panic("FIXME: implement delete");
    }
};

const Entry = struct {
    block: u16 = 0,
    record: u16 = 0,
};
