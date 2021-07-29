const std = @import("std");
const Store = @import("file.zig").Store;
const Page = @import("page.zig").Page;
const Entry = @import("entry.zig").Entry;

// Storage managers manage access to individual files.
// Each file is carved up into pages which we treat as a generic heap.
// TODO track occupancy using an occupancy map rather than a list of pages.
// This manager will never reclaim space.
//
// FILE HEADER
// - u16 block size
// - u16 number of pages
// - number of pages bytes for the occupancy map

pub const FileHeader = struct {
    block_size: u16 = 0,
    allocated_pages: u64 = 0,
};

pub const Error = error {
    PageDoesntExist,
};

pub const Manager = struct {
    header: FileHeader,
    file: *Store,
    file_size: u64,
    mem: *std.mem.Allocator,
    // TODO: cache pages in memory instead of reading and writing constantly. Performance
    // optimization
    pages: std.ArrayList(*Page),

    const Self = @This();


    fn findBlock(self: *Self, entry: Entry) callconv(.Inline) !*Page {
        if (entry.block > self.pages.items.len) {
            return Error.PageDoesntExist;
        }
        return self.pages.items[entry.block];
    }

    pub fn init(file: *Store, block_size: u16, mem: *std.mem.Allocator) !*Self {
        var hdr = FileHeader{.block_size = block_size};
        var pages = std.ArrayList(*Page).init(mem);
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

            try pages.resize(hdr.allocated_pages);

            var offset: u64 = @sizeOf(FileHeader);
            var i: usize = 0;
            const sz = @sizeOf(FileHeader) + block_size * hdr.allocated_pages;
            while (offset < sz) : (offset += block_size) {
                const b= try Page.init(file, offset, block_size, mem);
                pages.items[i] = b;
                i += 1;
            }
        }
        const mgr = try mem.create(Self);
        mgr.header = hdr;
        mgr.file = file;
        mgr.file_size = file_size;
        mgr.mem = mem;
        mgr.pages = pages;

        return mgr;
    }

    pub fn deinit(self: *Self) !void {
        std.debug.assert(self.header.allocated_pages == self.pages.items.len);
        try self.file.seekTo(0);
        try self.file.writeAll(std.mem.asBytes(&self.header));
        for (self.pages.items) |b| {
            try b.deinit();
        }
        self.pages.deinit();
        self.mem.destroy(self);
    }

    pub fn put(self: *Self, record: []const u8) !Entry {
        var i: u64 = 0;
        var end = self.pages.items.len;
        while (i < end) : (i += 1) {
            const b = self.pages.items[i];
            if (b.can_contain(@intCast(u16, record.len))) {
                break;
            }
        }
        const bl = blk: {
            if (i == end) {
                // No pages found, alloc a new one
                const b = try Page.init(self.file, self.file_size, self.header.block_size, self.mem);
                self.file_size += self.header.block_size;
                try self.file.extend(self.file_size);
                try self.pages.append(b);
                self.header.allocated_pages += 1;
                break :blk b;
            }
            break :blk self.pages.items[i];
        };
        // TODO if block is out of space update occupancy and make a new one
        var entry = try bl.put(record);
        entry.block = @intCast(u16, i);
        return entry;
    }

    pub fn get(self: *Self, entry: Entry) ![]const u8 {
        const b = try findBlock(self, entry);
        return b.get(entry.slot);
    }

    pub fn update(self: *Self, oldEntry: Entry, record: []const u8) !void {
        const bl = try findBlock(self, entry);
        try bl.delete(oldEntry.slot);
        var entry = try bl.put(record);
        entry.block = entry.block;
        return entry;
    }

    pub fn delete(self: *Self, entry: Entry) !void {
        const bl = try findBlock(self, entry);
        try bl.delete(oldEntry.slot);
    }
};
