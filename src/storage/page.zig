const std = @import("std");
const Entry = @import("entry.zig").Entry;
const FileManager = @import("file.zig").Manager;
const Latch = @import("libdb").sync.Latch;
const LatchHold = @import("libdb").sync.LatchHold;
const assert = std.debug.assert;
const PAGE_SIZE = @import("config.zig").PAGE_SIZE;

/// A single page of the directory.
/// The page directory is composed of a linked list of DirectoryPage structures
pub const DirectoryPage = struct {
    pageID: u16,
    pages: u16,
    next: u32,
    occupancy: []u8,

    pub fn init(self: *align(1) @This(), pageID: u16, pageSz: u16) void {
        self.pageID = pageID;
        self.pages = (pageSz - @sizeOf(u32)) / @sizeOf(u8);
        self.next = (pageID + self.pages) * pageSz;
    }
};

// Each block is of fixed length and acts as a bump-up
// allocator for contained records.
// A periodic compaction (or vacuum) process should clean up
// deleted blocks so there are no gaps in the file.
pub const Page = struct {
    live: bool = false,
    file: *FileManager,
    id: u64 = 0,
    lastAccess: usize = 0,
    pins: u64 = 0,
    dirty: bool = false,
    latch: *Latch,
    mem: std.mem.Allocator,
    buffer: []u8 = &[_]u8{},

    const Self = @This();

    pub fn init(self: *Self, mem: std.mem.Allocator) !void {
        self.mem = mem;
        self.latch = try Latch.init(self.mem);
        self.buffer = try mem.alignedAlloc(u8, PAGE_SIZE, PAGE_SIZE);
        self.live = false;
    }

    pub fn pinned(self: *Self) bool {
        return @atomicLoad(u64, &self.pins, .Acquire) > 0;
    }

    pub fn pin(self: *Self) void {
        _ = @atomicRmw(u64, &self.pins, .Add, 1, .Acquire);
    }

    pub fn unpin(self: *@This()) void {
        assert(self.pins >= 0);
        _ = @atomicRmw(u64, &self.pins, .Sub, 1, .Release);
    }

    pub fn deinit(self: *Self) !void {
        assert(!self.dirty);
        self.mem.free(self.buffer);
        self.mem.destroy(self.latch);
    }

    pub fn as(self: *Self, comptime T: type) type {
        return @ptrCast(*T, @alignCast(@alignOf(T), self.buffer));
    }
};
