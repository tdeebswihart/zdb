const std = @import("std");
const FileManager = @import("file.zig").Manager;
const Page = @import("page.zig").Page;
const LatchedPage = @import("page.zig").LatchedPage;
const PinnedPage = @import("page.zig").Pin;
const SharedPage = @import("page.zig").SharedPage;
const ExclusivePage = @import("page.zig").ExclusivePage;
const Latch = @import("libdb").sync.Latch;
const PAGE_SIZE = @import("config.zig").PAGE_SIZE;

const PageMetadata = struct {
    // page offset = sizeof header + sizeof directory + offset into directory *
    // pagesize?
    bytesFree: u16,
};

// Storage managers manage access to individual files.
// Each file is carved up into pages which we treat as a generic heap.
// TODO track occupancy using an occupancy map rather than a list of pages.
// This manager will never reclaim space.

// The storage manager only works with one file
pub const Manager = struct {
    file: FileManager,
    mem: std.mem.Allocator,
    pages: []Page,
    buffer: []u8,
    op: u64 = 0,
    latch: *Latch,

    const Self = @This();

    pub const Error = error{
        Full,
    };

    pub fn init(file: FileManager, nPages: usize, mem: std.mem.Allocator) !*Self {
        var pages = try mem.alloc(Page, nPages);

        var i: usize = 0;
        while (i < nPages) : (i += 1) {
            try pages[i].init(mem);
        }
        const mgr = try mem.create(Self);
        mgr.file = file;
        mgr.mem = mem;
        mgr.pages = pages;
        mgr.latch = try Latch.init(mem);

        return mgr;
    }

    pub fn deinit(self: *Self) !void {
        var i: usize = 0;
        _ = self.latch.exclusive();
        while (i < self.pages.len) : (i += 1) {
            var page = &self.pages[i];
            try self.writeback(page);
            try page.deinit();
        }
        self.mem.free(self.pages);
        self.mem.destroy(self.latch);
        self.mem.destroy(self);
        //self.* = undefined;
    }

    fn writeback(self: *Self, page: *Page) !void {
        var hold = page.latch.exclusive();
        defer hold.release();
        if (page.live and page.dirty) {
            _ = try self.file.writeAll(page.id, page.buffer[0..]);
            page.dirty = false;
        }
    }

    pub fn pin(self: *Self, pageID: u32) anyerror!*Page {
        var leastRecentlyUsed: usize = 0;
        var lowestTs: usize = std.math.maxInt(usize);
        var i: u16 = 0;

        // TODO: can I better handle concurrent `pin` operations for unloaded pages?
        // I want to avoid the situation where a page is loaded into two slots
        // The current solution is to xlock the manager while we pin
        // and use that to serialize the loading and unloading of
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
                page.pin();
                return page;
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
        try self.writeback(lru);
        _ = try self.file.readAll(pageID, lru.buffer[0..]);
        lru.id = pageID;
        lru.dirty = false;
        lru.live = true;
        lru.pin();
        return lru;
    }

    pub fn pinLatched(self: *Self, pageID: u32, kind: Latch.Kind) anyerror!LatchedPage {
        const page = try self.pin(pageID);
        var hold = switch (kind) {
            .shared => page.latch.shared(),
            .exclusive => page.latch.exclusive(),
        };
        return LatchedPage{
            .page = page,
            .hold = hold,
        };
    }
};
