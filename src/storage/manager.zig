const std = @import("std");
const Store = @import("file.zig").Store;
const Page = @import("page.zig").Page;
const PinnedPage = @import("page.zig").Pin;
const SharedPage = @import("page.zig").SharedPage;
const ExclusivePage = @import("page.zig").ExclusivePage;
const Entry = @import("entry.zig").Entry;
const Latch = @import("libdb").sync.Latch;

const storageVersion = 1;

const PageMetadata = struct {
    // page offset = sizeof header + sizeof directory + offset into directory *
    // pagesize?
    bytesFree: u16,
};

// Storage managers manage access to individual files.
// Each file is carved up into pages which we treat as a generic heap.
// TODO track occupancy using an occupancy map rather than a list of pages.
// This manager will never reclaim space.
//
// FILE HEADER
// - u16 block size
// - u16 number of pages
// - number of pages bytes for the occupancy map
pub const Header = struct {
    version: u16 = storageVersion,
    page_size: u16 = 0,
    pagesAllocated: u64 = 0,
};

pub const Error = error{
    Full,
};

// The storage manager only works with one file
pub const Manager = struct {
    header: Header,
    file: *Store,
    file_size: u64,
    page_size: u16,
    mem: *std.mem.Allocator,
    pages: []Page,
    buffer: []u8,
    op: u64 = 0,
    latch: *Latch,

    const Self = @This();

    pub fn init(file: *Store, page_size: u16, buffer_size: usize, mem: *std.mem.Allocator) !*Self {
        var hdr = Header{ .page_size = page_size };
        const nPages = buffer_size / page_size;
        var pages = try mem.alloc(Page, nPages);
        var buffer = try mem.alloc(u8, buffer_size);

        var i: usize = 0;
        while (i < nPages) : (i += 1) {
            try pages[i].init(mem, buffer[i * page_size .. (i + 1) * page_size]);
        }

        var file_size: u64 = 0;

        const size = try file.size();
        if (size == 0) {
            // New file, write our initial header
            try file.writeAll(std.mem.asBytes(&hdr));
            file_size = @sizeOf(Header);
        } else {
            file_size = size;
            // read existing header
            try file.seekTo(0);
            _ = try file.read(std.mem.asBytes(&hdr));
            std.debug.assert(hdr.page_size == page_size);
        }
        // TODO: read the first page into memory and retrieve occupancy details
        // TODO: how do I keep the directory in memory? must I pin/unpin it? this is messy...
        // Could have the page directory be a linked list of pages that we treat as normal but
        // that gets messy here...
        // I'd prefer to forcibly map the root of the page directory and be done
        // with it as that simplifies things.
        // Once I have a working b-tree implementation that shouldn't be hard
        // (I say that now)...
        const mgr = try mem.create(Self);
        mgr.header = hdr;
        mgr.file = file;
        mgr.file_size = file_size;
        mgr.mem = mem;
        mgr.pages = pages;
        mgr.buffer = buffer;
        mgr.page_size = page_size;
        mgr.latch = try Latch.init(mem);

        return mgr;
    }

    pub fn deinit(self: *Self) !void {
        try self.file.seekTo(0);
        try self.file.writeAll(std.mem.asBytes(&self.header));
        var i: usize = 0;
        while (i < self.pages.len) : (i += 1) {
            var page = &self.pages[i];
            try page.deinit();
        }
        _ = self.latch.exclusive();
        self.mem.free(self.pages);
        self.mem.free(self.buffer);
        self.mem.destroy(self.latch);
        self.mem.destroy(self);
    }

    pub fn pin(self: *Self, pageID: u64) !PinnedPage {
        var leastRecentlyUsed: usize = 0;
        var lowestTs: usize = std.math.maxInt(usize);
        var i: u16 = 0;

        // TODO: can I better handle concurrent `pin` operations for unloaded pages?
        // I want to avoid the situation where a page is loaded into two slots
        // The currenty solution is to xlock the manager while we perform pin and unpin
        // operations, and use that to serialize the loading and unloading of
        // pages that have spilled to disk
        var hold = self.latch.exclusive();
        defer hold.release();
        while (i < self.pages.len) : (i += 1) {
            const page = &self.pages[i];
            if (!page.live) {
                lowestTs = 0;
                leastRecentlyUsed = i;
                break;
            }
            if (page.id == pageID) {
                return page.pin();
            }
            if (!page.pinned() and page.lastAccess < lowestTs) {
                lowestTs = page.lastAccess;
                leastRecentlyUsed = i;
            }
        }
        if (lowestTs == std.math.maxInt(usize)) {
            // No unpinned pages
            return Error.Full;
        }
        var lru = &self.pages[leastRecentlyUsed];
        try lru.writeback();
        const offset = @sizeOf(Header) + self.page_size * pageID;
        try lru.reinit(self.file, pageID, offset, self.page_size);
        return lru.pin();
    }
};

fn setup() !std.fs.File {
    const tmp = try std.fs.openDirAbsolute("/tmp", .{});
    return try tmp.createFile("test.zdb", .{
        .read = true,
        .truncate = true,
        .lock = .Exclusive,
    });
}

const t = std.testing;
const File = @import("file.zig").File;
test "pages can be written to" {
    const dbfile = try setup();
    defer dbfile.close();
    var fs = try File.init(dbfile);
    const mgr = try Manager.init(&fs.store, 512, 1024, t.allocator);

    const expected: []const u8 = &[_]u8{ 0x41, 0x42, 0x43 };
    var page = try mgr.pin(0);

    var xPage = page.exclusive();
    const loc = try xPage.put(expected);
    xPage.deinit();
    var shared = page.shared();
    const found = try shared.get(loc.slot);
    try t.expectEqualSlices(u8, found, expected);
    shared.deinit();
    page.unpin();
    try mgr.deinit();
}

test "pinned pages are not evicted" {
    const dbfile = try setup();
    defer dbfile.close();
    var fs = try File.init(dbfile);
    const mgr = try Manager.init(&fs.store, 128, 128, t.allocator);

    var page = try mgr.pin(0);
    try t.expectError(Error.Full, mgr.pin(1));
    page.unpin();
    try mgr.deinit();
}
