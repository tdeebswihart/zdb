const std = @import("std");
const FileManager = @import("file.zig").Manager;
const page = @import("page.zig");
const LatchedPage = @import("page.zig").LatchedPage;
const PinnedPage = @import("page.zig").Pin;
const SharedPage = @import("page.zig").SharedPage;
const ExclusivePage = @import("page.zig").ExclusivePage;
const lib = @import("libdb");
const Latch = lib.sync.Latch;
const PAGE_SIZE = @import("config.zig").PAGE_SIZE;
const assert = std.debug.assert;

const log = std.log.scoped(.bm);

fn Result(comptime T: type) type {
    return lib.Result(T, anyerror);
}

// Storage managers manage access to individual files.
// Each file is carved up into pages which we treat as a generic heap.
// TODO track occupancy using an occupancy map rather than a list of pages.
// This manager will never reclaim space.

// The storage manager only works with one file
pub const Manager = struct {
    file: FileManager,
    mem: std.mem.Allocator,
    headPage: *page.ControlBlock,
    directoryHead: *DirectoryPage,
    pages: []page.ControlBlock,
    op: u64 = 0,
    latch: Latch = .{},

    const Self = @This();

    pub const Error = error{
        Full,
        WrongDirectory,
        PageNotFound,
        Invalid,
        TooSmall,
        PageTypeMismatch,
        CannotFree,
    };

    pub fn init(file: FileManager, size: usize, mem: std.mem.Allocator) !*Self {
        var pages = try mem.alloc(page.ControlBlock, size);

        var i: usize = 0;
        while (i < size) : (i += 1) {
            try pages[i].init();
        }
        const mgr = try mem.create(Self);
        mgr.file = file;
        mgr.mem = mem;
        mgr.pages = pages;
        mgr.latch = .{};
        mgr.headPage = try mgr.pin(0, .directory);
        var hdr = mgr.headPage.header();
        if (hdr.magic != page.MAGIC) {
            hdr.magic = page.MAGIC;
            hdr.pageID = 0;
            hdr.pageType = .directory;
            hdr.lsn = 0;
            hdr.crc32 = 0;
            mgr.directoryHead = DirectoryPage.new(mgr.headPage);
        } else {
            mgr.directoryHead = DirectoryPage.from(mgr.headPage);
        }

        return mgr;
    }

    pub fn deinit(self: *Self) !void {
        var i: usize = 0;
        _ = self.latch.exclusive();
        self.headPage.unpin();
        while (i < self.pages.len) : (i += 1) {
            var p = &self.pages[i];
            try self.writebackLatched(p);
            try p.deinit();
        }
        self.mem.free(self.pages);
        self.mem.destroy(self);
    }

    fn writebackLatched(self: *Self, p: *page.ControlBlock) !void {
        if (p.live and p.dirty) {
            const hdr = p.header();
            _ = try self.file.writeAll(hdr.pageID, p.buffer[0..]);
            p.dirty = false;
        }
    }

    fn writeback(self: *Self, p: *page.ControlBlock) !void {
        log.debug("writeback page={d}({any})", .{ p.id(), p });
        // allow reads while we write a page back
        var hold = p.latch.shared();
        defer hold.release();
        try self.writebackLatched(p);
    }
    pub fn pin(self: *Self, pageID: u32, pageTy: page.Type) anyerror!*page.ControlBlock {
        var hold = self.latch.exclusive();
        defer hold.release();
        return self.pinImpl(pageID, pageTy);
    }

    fn pinImpl(self: *Self, pageID: u32, pageTy: ?page.Type) anyerror!*page.ControlBlock {
        var leastRecentlyUsed: usize = 0;
        var lowestTs: usize = std.math.maxInt(usize);
        var i: u16 = 0;

        // TODO: can I better handle concurrent `pin` operations for unloaded pages?
        // I want to avoid the situation where a page is loaded into two slots
        // The current solution is to xlock the manager while we pin
        // and use that to serialize the loading and unloading of
        // pages that have spilled to disk
        while (i < self.pages.len) : (i += 1) {
            const p = &self.pages[i];
            if (!p.live) {
                lowestTs = 0;
                leastRecentlyUsed = i;
                break;
            }
            const hdr = p.header();
            if (hdr.pageID == pageID) {
                if (pageTy) |pty| {
                    if (hdr.pageType != .free and hdr.pageType != pty) {
                        log.err("mismatched page={d} expected type={any} found={any}", .{ pageID, pty, hdr.pageType });
                        return Error.PageTypeMismatch;
                    }
                }
                p.pin();
                return p;
            }
            if (!p.pinned() and p.lastAccess < lowestTs) {
                lowestTs = p.lastAccess;
                leastRecentlyUsed = i;
            }
        }
        if (lowestTs == std.math.maxInt(usize)) {
            // No unpinned pages
            return Error.Full;
        }
        var lru = &self.pages[leastRecentlyUsed];
        if (lru.live) {
            try self.writeback(lru);
        }
        _ = try self.file.readAll(pageID, lru.buffer[0..lru.buffer.len]);
        var hdr = lru.header();
        if (hdr.magic == page.MAGIC) {
            if (hdr.pageID != pageID) {
                log.err("loaded page={d} as page={d}", .{ hdr.pageID, pageID });
                return Error.Invalid;
            }
            // only check allocated pages as this is also used internally by alloc
            if (pageTy) |pty| {
                if (pty != hdr.pageType) {
                    return Error.PageTypeMismatch;
                }
            }
            // FIXME check crc32 if valid?
        }
        self.op += 1;
        lru.reinit(self.op);
        lru.pin();
        return lru;
    }

    pub fn pinLatched(self: *Self, pageID: u32, pageTy: page.Type, kind: Latch.Kind) anyerror!LatchedPage {
        const p = try self.pin(pageID, pageTy);
        var hold = switch (kind) {
            .shared => p.latch.shared(),
            .exclusive => p.latch.exclusive(),
        };
        return LatchedPage{
            .page = p,
            .hold = hold,
        };
    }

    /// Allocate and pin a page
    pub fn allocate(self: *Self, pageTy: page.Type) anyerror!*page.ControlBlock {
        var hold = self.latch.exclusive();
        defer hold.release();
        var p = self.headPage;
        var dir = self.directoryHead;
        var pageHold = p.latch.shared();
        while (dir.full()) {
            var isNew = false;
            const next = dir.header.pageID + nPages;
            if (dir.next == 0) {
                // new page
                dir.next = next;
                p.dirty = true;
                isNew = true;
            }
            pageHold.release();
            if (dir != self.directoryHead) {
                p.unpin();
            }
            p = try self.pinImpl(next, .directory);
            pageHold = p.latch.exclusive();
            if (isNew) {
                dir = DirectoryPage.new(p);
            } else {
                dir = DirectoryPage.from(p);
            }
        }

        const pageID = dir.allocate() orelse {
            assert(false);
            unreachable;
        };
        log.debug("allocated page={d} type={any}", .{ pageID, pageTy });
        p.dirty = true;

        pageHold.release();
        if (dir != self.directoryHead) {
            p.unpin();
        }
        var np = try self.pinImpl(pageID, pageTy);
        var hdr = np.header();
        hdr.magic = page.MAGIC;
        hdr.pageID = pageID;
        hdr.pageType = pageTy;
        hdr.lsn = 0;
        hdr.crc32 = 0;
        return np;
    }

    pub fn allocLatched(self: *Self, pageTy: page.Type, kind: Latch.Kind) anyerror!LatchedPage {
        const p = try self.allocate(pageTy);
        var hold = switch (kind) {
            .shared => p.latch.shared(),
            .exclusive => p.latch.exclusive(),
        };
        return LatchedPage{
            .page = p,
            .hold = hold,
        };
    }

    pub fn free(self: *Self, pageID: u32) !void {
        var hold = self.latch.exclusive();
        defer hold.release();
        var dirPage = self.headPage;
        var dir = self.directoryHead;
        var pageHold = dirPage.latch.shared();
        while (pageID > dir.header.pageID + nPages) {
            const next = dir.next;
            pageHold.release();
            if (dir != self.directoryHead) {
                dirPage.unpin();
            }
            if (next == 0) {
                return Error.PageNotFound;
            }
            // go to the next directory
            dirPage = try self.pinImpl(dir.next, .directory);
            pageHold = dirPage.latch.shared();
            dir = DirectoryPage.from(dirPage);
        }
        // found the right page
        pageHold.release();
        pageHold = dirPage.latch.exclusive();

        log.debug("freeing page={d} dir={d}", .{ pageID, dirPage.id() });
        dir.free(pageID);
        var p = try self.pinImpl(pageID, null);
        defer p.unpin();
        var ph = p.latch.exclusive();
        defer ph.release();
        if (p.pins > 1) {
            log.err("cannot free page={d} with pins={d}", .{ pageID, p.pins });
            return Error.CannotFree;
        }
        var hdr = p.header();
        // Scribble out the page's contents?
        hdr.pageType = .free;
        dirPage.dirty = true;

        pageHold.release();
        if (dir != self.directoryHead) {
            dirPage.unpin();
        }
    }
};

// page_size - directory overhead
const nPages = (PAGE_SIZE - @sizeOf(page.Header) - @sizeOf(u32)) / 8;
fn set_bit(bit: u8) u8 {
    return @as(u8, 1) << (7 - @truncate(u3, bit));
}
/// A single page of the directory.
/// The page directory is composed of a linked list of DirectoryPage structures
pub const DirectoryPage = packed struct {
    // The number of pages a directoryPage can manage
    header: page.Header,
    next: u32,
    freePages: [nPages]u8,

    const Self = @This();

    pub fn from(p: *page.ControlBlock) *Self {
        comptime assert(@sizeOf(Self) <= PAGE_SIZE);
        return @ptrCast(*Self, @alignCast(@alignOf(Self), p.buffer[0..]));
    }

    pub fn new(p: *page.ControlBlock) *Self {
        var self = Self.from(p);
        // Fuck it. I'll use u64s and bit math some other time
        for (self.freePages) |_, idx| {
            self.freePages[idx] = std.math.maxInt(u8);
        }
        p.dirty = true;
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

    pub fn allocate(self: *Self) ?u32 {
        for (self.freePages) |available, idx| {
            if (available > 0) {
                var i: u8 = 0;
                var offset: u32 = std.math.maxInt(u32);
                // Find the first set bit
                while (i < 8) {
                    if (available & set_bit(i) > 0) {
                        offset = i;
                        break;
                    }
                    i += 1;
                }
                assert(offset < 8);
                const pageID: u32 = self.header.pageID + @intCast(u32, idx) * 8 + offset + 1;
                log.debug("allocated page={d} byte={d} bit={d}", .{ pageID, idx, offset });
                // clear that bit
                self.freePages[idx] &= ~set_bit(i);
                return pageID;
            }
        }
        return null;
    }

    pub fn free(self: *Self, pageID: u64) void {
        const offset = pageID - self.header.pageID - 1;
        self.freePages[offset / 8] |= set_bit(@truncate(u3, offset));
        self.freePages[offset / 8] |= set_bit(@truncate(u3, offset));
        log.debug("freed page={d} byte={d} bit={d}", .{ pageID, offset / 8, @truncate(u3, offset) });
    }
};
