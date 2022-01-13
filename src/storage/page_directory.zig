const std = @import("std");
const Latch = @import("libdb").sync.Latch;
const LatchHold = @import("libdb").sync.LatchHold;
const assert = std.debug.assert;
const PAGE_SIZE = @import("config.zig").PAGE_SIZE;
const buffer = @import("buffer.zig");
const Page = @import("page.zig").Page;

pub const Error = error{
    WrongDirectory,
    Invalid,
};

pub const Directory = struct {
    headPage: ?*Page,
    head: *DirectoryPage,
    latch: *Latch,
    mem: std.mem.Allocator,
    bufmgr: *buffer.Manager,

    const Self = @This();

    pub fn init(mem: std.mem.Allocator, bufmgr: *buffer.Manager) !Self {
        var headPage = try bufmgr.pin(0);
        var dir = Directory{
            .headPage = headPage,
            .head = DirectoryPage.from(headPage),
            .latch = try Latch.init(mem),
            .mem = mem,
            .bufmgr = bufmgr,
        };
        return dir;
    }

    pub fn deinit(self: *Self) void {
        var hold = self.latch.exclusive();
        defer hold.release();
        var headPage = self.headPage orelse {
            return;
        };

        headPage.unpin();
        self.headPage = null;
        self.mem.destroy(self.latch);
    }

    pub fn allocate(self: Self) anyerror!*Page {
        var hold = self.latch.shared();
        defer hold.release();
        var page = self.headPage orelse {
            assert(false);
            return Error.Invalid;
        };
        var dir = self.head;
        var pageHold: LatchHold = page.latch.shared();
        while (dir.full()) {
            pageHold.release();
            if (dir != self.head) {
                page.unpin();
            }
            // go to the next directory
            page = try self.bufmgr.pin(dir.next);
            pageHold = page.latch.exclusive();
            dir = DirectoryPage.from(page);
        }

        const pageID = dir.allocate() orelse {
            assert(false);
            unreachable;
        };
        page.dirty = true;

        pageHold.release();
        if (dir != self.head) {
            page.unpin();
        }
        return try self.bufmgr.pin(pageID);
    }

    pub fn free(self: Self, pageID: u64) void {
        var hold = self.latch.shared();
        defer hold.release();
        var dirPage = self.headPage orelse {
            assert(false);
            return;
        };
        var dir = self.head;
        var pageHold: LatchHold = dirPage.latch.shared();
        while (pageID > dir.id + nPages) {
            pageHold.release();
            if (dir != self.head) {
                dir.unpin();
            }
            // go to the next dirPageectory
            dirPage = try self.bufmgr.pin(dirPage.next);
            pageHold = dirPage.latch.shared();
            dir = DirectoryPage.from(dirPage);
        }
        // found the right page
        pageHold.release();
        pageHold = dir.exclusive();

        dir.free(pageID);
        // Scribble out the page's contents
        @memset(dirPage.buffer, 0x41, PAGE_SIZE);
        dirPage.dirty = true;

        pageHold.release();
        if (dir != self.head) {
            dirPage.unpin();
        }
    }
};

// page_size - directory overhead
const nPages = (PAGE_SIZE - 16);

/// A single page of the directory.
/// The page directory is composed of a linked list of DirectoryPage structures
pub const DirectoryPage = struct {
    // The number of pages a directoryPage can manage
    id: u64,
    next: u64,
    freePages: [nPages]u1,

    const Self = @This();

    pub fn from(page: *Page) *Self {
        var self = @ptrCast(*Self, @alignCast(@alignOf(Self), page.buffer));
        if (self.id != page.id) {
            // This is a new page
            self.id = page.id;
            self.next = page.id + nPages;
            // Fuck it. I'll use u64s and bit math some other time
            for (self.freePages) |_, idx| {
                self.freePages[idx] = 1;
            }
            page.dirty = true;
        }
        return self;
    }

    pub fn full(self: *Self) bool {
        for (self.freePages) |v| {
            if (v > 0) {
                return false;
            }
        }
        return true;
    }

    pub fn allocate(self: *Self) ?u64 {
        for (self.freePages) |available, idx| {
            if (available == 1) {
                const pageID = self.id + idx + 1;
                self.freePages[idx] = 0;
                return pageID;
            }
        }
        return null;
    }

    pub fn free(self: *Self, pageID: u64) void {
        const offset = pageID - self.id;
        self.freePages[offset] = 1;
    }
};
