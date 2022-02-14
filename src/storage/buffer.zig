const std = @import("std");
const FileManager = @import("file.zig").Manager;
const Page = @import("page.zig").Page;
const LatchedPage = @import("page.zig").LatchedPage;
const PinnedPage = @import("page.zig").Pin;
const SharedPage = @import("page.zig").SharedPage;
const ExclusivePage = @import("page.zig").ExclusivePage;
const Latch = @import("libdb").sync.Latch;
const PAGE_SIZE = @import("config.zig").PAGE_SIZE;
const assert = std.debug.assert;

const log = std.log.scoped(.bm);

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
    headPage: *Page,
    directoryHead: *DirectoryPage,
    pages: []Page,
    buffer: []u8,
    op: u64 = 0,
    latch: Latch = .{},

    const Self = @This();

    pub const Error = error{
        Full,
        WrongDirectory,
        Invalid,
        TooSmall,
    };

    pub fn init(file: FileManager, size: usize, mem: std.mem.Allocator) !*Self {
        var pages = try mem.alloc(Page, size);

        var i: usize = 0;
        while (i < size) : (i += 1) {
            try pages[i].init(mem);
        }
        const mgr = try mem.create(Self);
        mgr.file = file;
        mgr.mem = mem;
        mgr.pages = pages;
        mgr.latch = .{};
        mgr.headPage = try mgr.pin(0);
        mgr.directoryHead = DirectoryPage.from(mgr.headPage);

        return mgr;
    }

    pub fn deinit(self: *Self) !void {
        var i: usize = 0;
        _ = self.latch.exclusive();
        self.headPage.unpin();
        while (i < self.pages.len) : (i += 1) {
            var page = &self.pages[i];
            try self.writeback(page);
            try page.deinit();
        }
        self.mem.free(self.pages);
        self.mem.destroy(self);
    }

    fn writeback(self: *Self, page: *Page) !void {
        // allow reads while we write a page back
        var hold = page.latch.shared();
        defer hold.release();
        if (page.live and page.dirty) {
            _ = try self.file.writeAll(page.id, page.buffer[0..]);
            page.dirty = false;
        }
    }
    pub fn pin(self: *Self, pageID: u32) anyerror!*Page {
        var hold = self.latch.exclusive();
        defer hold.release();
        return self.pinImpl(pageID);
    }

    fn pinImpl(self: *Self, pageID: u32) anyerror!*Page {
        var leastRecentlyUsed: usize = 0;
        var lowestTs: usize = std.math.maxInt(usize);
        var i: u16 = 0;

        // TODO: can I better handle concurrent `pin` operations for unloaded pages?
        // I want to avoid the situation where a page is loaded into two slots
        // The current solution is to xlock the manager while we pin
        // and use that to serialize the loading and unloading of
        // pages that have spilled to disk
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
        _ = try self.file.readAll(pageID, lru.buffer[0..lru.buffer.len]);
        lru.id = pageID;
        lru.dirty = false;
        lru.live = true;
        lru.pins = 0;
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

    /// Allocate and pin a page
    pub fn allocate(self: *Self) anyerror!*Page {
        var hold = self.latch.exclusive();
        defer hold.release();
        var page = self.headPage;
        var dir = self.directoryHead;
        var pageHold = page.latch.shared();
        while (dir.full()) {
            pageHold.release();
            if (dir != self.directoryHead) {
                page.unpin();
            }
            // go to the next directory
            page = try self.pinImpl(dir.next);
            pageHold = page.latch.exclusive();
            dir = DirectoryPage.from(page);
        }

        const pageID = dir.allocate() orelse {
            assert(false);
            unreachable;
        };
        page.dirty = true;

        pageHold.release();
        if (dir != self.directoryHead) {
            page.unpin();
        }
        return try self.pinImpl(pageID);
    }

    pub fn allocLatched(self: *Self, kind: Latch.Kind) anyerror!LatchedPage {
        const page = try self.allocate();
        var hold = switch (kind) {
            .shared => page.latch.shared(),
            .exclusive => page.latch.exclusive(),
        };
        return LatchedPage{
            .page = page,
            .hold = hold,
        };
    }

    pub fn free(self: *Self, pageID: u32) !void {
        var hold = self.latch.exclusive();
        defer hold.release();
        var dirPage = self.headPage;
        var dir = self.directoryHead;
        var pageHold = dirPage.latch.shared();
        while (pageID > dir.id + nPages) {
            pageHold.release();
            if (dir != self.directoryHead) {
                dirPage.unpin();
            }
            // go to the next dirPageectory
            dirPage = try self.pinImpl(dir.next);
            pageHold = dirPage.latch.shared();
            dir = DirectoryPage.from(dirPage);
        }
        // found the right page
        pageHold.release();
        pageHold = dirPage.latch.exclusive();

        dir.free(pageID);
        // Scribble out the page's contents
        var page = try self.pinImpl(pageID);
        defer page.unpin();
        var ph = page.latch.exclusive();
        defer ph.release();
        for (page.buffer) |*b| b.* = 0x41;
        dirPage.dirty = true;

        pageHold.release();
        if (dir != self.directoryHead) {
            dirPage.unpin();
        }
    }
};

// page_size - directory overhead
const nPages = (PAGE_SIZE - @sizeOf(u32) * 3);
const dirMagic = 0xC45C4DE;
/// A single page of the directory.
/// The page directory is composed of a linked list of DirectoryPage structures
pub const DirectoryPage = struct {
    // The number of pages a directoryPage can manage
    id: u32,
    next: u32,
    magic: u32,
    freePages: [nPages]u1,

    const Self = @This();

    pub fn from(page: *Page) *Self {
        var self = @ptrCast(*Self, @alignCast(@alignOf(Self), page.buffer[0..]));
        if (self.magic != dirMagic) {
            // This is a new page
            self.id = page.id;
            self.magic = dirMagic;
            self.next = page.id + nPages;
            // Fuck it. I'll use u64s and bit math some other time
            for (self.freePages) |_, idx| {
                self.freePages[idx] = 1;
            }
            page.dirty = true;
        } else if (self.id != page.id) {
            @panic("corrupt directory page");
        }
        return self;
    }

    pub fn full(self: *Self) bool {
        for (self.freePages) |v| {
            if (v == 1) {
                return false;
            }
        }
        return true;
    }

    pub fn allocate(self: *Self) ?u32 {
        for (self.freePages) |available, idx| {
            if (available == 1) {
                const pageID: u32 = self.id + @intCast(u32, idx) + 1;
                self.freePages[idx] = 0;
                return pageID;
            }
        }
        return null;
    }

    pub fn free(self: *Self, pageID: u64) void {
        const offset = pageID - self.id - 1;
        self.freePages[offset] = 1;
    }
};
