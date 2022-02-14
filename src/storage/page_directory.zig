const std = @import("std");
const Latch = @import("libdb").sync.Latch;
const assert = std.debug.assert;
const PAGE_SIZE = @import("config.zig").PAGE_SIZE;
const buffer = @import("buffer.zig");
const Page = @import("page.zig").Page;
const LatchedPage = @import("page.zig").LatchedPage;

const log = std.log.scoped(.pd);

pub const Error = error{
    WrongDirectory,
    Invalid,
};

pub const Directory = struct {
    headPage: ?*Page,
    head: *DirectoryPage,
    latch: Latch,
    mem: std.mem.Allocator,
    bufmgr: *buffer.Manager,

    const Self = @This();

    pub fn init(mem: std.mem.Allocator, bufmgr: *buffer.Manager) !*Self {
        var dir: *Self = try mem.create(Self);
        var headPage = try bufmgr.pin(0);
        dir.headPage = headPage;
        dir.head = DirectoryPage.from(headPage);
        dir.mem = mem;
        dir.bufmgr = bufmgr;
        dir.latch = .{};
        return dir;
    }

    pub fn deinit(self: *Self) void {
        _ = self.latch.exclusive();
        var headPage = self.headPage orelse {
            return;
        };

        headPage.unpin();
        self.headPage = null;
        self.mem.destroy(self);
    }

    pub fn allocate(self: *Self) anyerror!*Page {
        var hold = self.latch.shared();
        defer hold.release();
        var page = self.headPage orelse {
            assert(false);
            return Error.Invalid;
        };
        var dir = self.head;
        var pageHold = page.latch.shared();
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
        log.debug("allocated={d} directory={d}", .{ pageID, dir.id });
        return try self.bufmgr.pin(pageID);
    }

    pub fn allocLatched(self: *Self, kind: Latch.Kind) anyerror!LatchedPage {
        const page = try self.allocate();
        var hold = switch (kind) {
            .shared => page.latch.shared(),
            .exclusive => page.latch.exclusive(),
        };
        log.debug("latched={d} kind={}", .{ page.id, kind });
        return LatchedPage{
            .page = page,
            .hold = hold,
        };
    }

    pub fn free(self: *Self, pageID: u32) !void {
        var hold = self.latch.shared();
        defer hold.release();
        var dirPage = self.headPage orelse {
            assert(false);
            return;
        };
        var dir = self.head;
        var pageHold = dirPage.latch.shared();
        while (pageID > dir.id + nPages) {
            pageHold.release();
            if (dir != self.head) {
                dirPage.unpin();
            }
            // go to the next dirPageectory
            dirPage = try self.bufmgr.pin(dir.next);
            pageHold = dirPage.latch.shared();
            dir = DirectoryPage.from(dirPage);
        }
        // found the right page
        pageHold.release();
        pageHold = dirPage.latch.exclusive();

        dir.free(pageID);
        // Scribble out the page's contents
        var page = try self.bufmgr.pin(pageID);
        defer page.unpin();
        var ph = page.latch.exclusive();
        defer ph.release();
        for (page.buffer) |*b| b.* = 0x41;
        dirPage.dirty = true;

        pageHold.release();
        if (dir != self.head) {
            dirPage.unpin();
        }
        log.debug("directory={d} freed={d}", .{ pageID, dirPage.id });
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
                log.debug("directory={d} allocated={d} offset={d}", .{ self.id, pageID, idx });
                return pageID;
            }
        }
        return null;
    }

    pub fn free(self: *Self, pageID: u64) void {
        const offset = pageID - self.id - 1;
        log.debug("directory={d} freed={d} offset={d}", .{ self.id, pageID, offset });
        self.freePages[offset] = 1;
    }
};
