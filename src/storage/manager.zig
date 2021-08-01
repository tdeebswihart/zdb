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
    block_size: u16,
    mem: *std.mem.Allocator,
    // TODO: cache pages in memory instead of reading and writing constantly. Performance
    // optimization
    pages: []Page,
    buffer: []u8,

    const Self = @This();


    fn findPage(self: *Self, id: u64) callconv(.Inline) !*Page {
        if (id > self.header.allocated_pages) {
            return Error.PageDoesntExist;
        }
        // TODO check if allocated
        var leastRecentlyUsed: usize = 0;
        var lowestTs: usize = std.math.maxInt(usize);
        for (self.pages) |page, i| {
            if (page.id == id) {
                return &self.pages[id];
            }
            if (page.lastAccess < lowestTs) {
                leastRecentlyUsed = i;
            }
        }
        // Not found, punt the least recently used page.
        var lru = &self.pages[leastRecentlyUsed];
        try lru.flush();
        const offset = @sizeOf(FileHeader) + self.block_size * id;
        try lru.reinit(self.file, id, offset, self.block_size);
        return lru;
    }

    pub fn init(file: *Store, block_size: u16, buffer_size: usize, mem: *std.mem.Allocator) !*Self {
        var hdr = FileHeader{.block_size = block_size};
        const nPages = buffer_size / block_size;
        var pages = try mem.alloc(Page, nPages);
        var buffer = try mem.alloc(u8, buffer_size);

        var i: usize = 0;
        while (i < nPages) : (i += 1){
            pages[i].buffer = buffer[i*block_size..(i+1)*block_size];
        }

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

            // var offset: u64 = @sizeOf(FileHeader);
            // var i: usize = 0;
            // const sz = @sizeOf(FileHeader) + block_size * hdr.allocated_pages;
            // while (offset < sz) : (offset += block_size) {
            //     const b= try Page.init(file, offset, block_size, mem);
            //     pages.items[i] = b;
            //     i += 1;
            // }
        }
        const mgr = try mem.create(Self);
        mgr.header = hdr;
        mgr.file = file;
        mgr.file_size = file_size;
        mgr.mem = mem;
        mgr.pages = pages;
        mgr.buffer = buffer;
        mgr.block_size = block_size;

        return mgr;
    }

    pub fn deinit(self: *Self) !void {
        try self.file.seekTo(0);
        try self.file.writeAll(std.mem.asBytes(&self.header));
        var i: usize = 0;
        while (i < self.pages.len) : (i += 1){
            var page = &self.pages[i];
            try page.deinit();
        }
        self.mem.free(self.pages);
        self.mem.free(self.buffer);
        self.mem.destroy(self);
    }

    pub fn put(self: *Self, record: []const u8) !Entry {
        // TODO track occupancy of _all_ pages in memory
        // so we can track one down if its not resident in the cache
        @panic("not updated for the cache yet");
        const sz = @intCast(u16, record.len);
        var bl = blk: {
            var i: usize = 0;
            while (i < self.pages.len) : (i+=1){
                const page = &self.pages[i];
                if (page.can_contain(sz)) {
                    break :blk page;
                }
            }
            // FIXME evict one.
            return error.OutOfSpace;
        };
        return try bl.put(record);
    }

    pub fn get(self: *Self, entry: Entry) ![]const u8 {
        const b = try findPage(self, entry.block);
        return b.get(entry.slot);
    }

    pub fn update(self: *Self, oldEntry: Entry, record: []const u8) !void {
        const bl = try findPage(self, entry.block);
        try bl.delete(oldEntry.slot);
        var entry = try bl.put(record);
        entry.block = entry.block;
        return entry;
    }

    pub fn delete(self: *Self, entry: Entry) !void {
        const bl = try findPage(self, entry.block);
        try bl.delete(oldEntry.slot);
    }
};
