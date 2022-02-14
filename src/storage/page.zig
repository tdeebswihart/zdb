const std = @import("std");
const Latch = @import("libdb").sync.Latch;
const PAGE_SIZE = @import("config.zig").PAGE_SIZE;
const assert = std.debug.assert;

const log = std.log.scoped(.page);

pub const LatchedPage = struct {
    page: *Page,
    hold: Latch.Hold,

    pub fn deinit(self: *@This()) void {
        self.hold.release();
        self.page.unpin();
        self.* = undefined;
    }
};

pub const PageHeader = struct {
    // Should be the checksum of everything in the page after it
    crc32: u32 = 0,
    lsn: u32 = 0,
};

// FIXME: this is actually the buffer control block, not the page.
// The page is at \.buffer
pub const Page = struct {
    live: bool = false,
    id: u32 = 0,
    lastAccess: usize = 0,
    pins: u64 = 0,
    lsn: u64 = 0,
    dirty: bool = false,
    latch: Latch = .{},
    mem: std.mem.Allocator,
    buffer: [PAGE_SIZE]u8,

    const Self = @This();

    pub fn init(self: *Self, mem: std.mem.Allocator) !void {
        self.mem = mem;
        self.live = false;
        self.pins = 0;
        self.dirty = false;
        self.lastAccess = 0;
        self.id = 0;
        self.latch = .{};
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
        self.* = undefined;
    }
};
