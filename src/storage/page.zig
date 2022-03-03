const std = @import("std");
const Latch = @import("libdb").sync.Latch;
const PAGE_SIZE = @import("config.zig").PAGE_SIZE;
const assert = std.debug.assert;

const log = std.log.scoped(.page);

pub const LatchedPage = struct {
    page: *ControlBlock,
    hold: Latch.Hold,

    pub fn deinit(self: *@This()) void {
        log.debug("released shares={d} page={d}", .{ self.hold.shares, self.page.id() });
        self.hold.release();
        self.page.unpin();
        self.* = undefined;
    }
};

pub const Type = enum(u8) { free, directory, hashDirectory, hashBucket, tuple };

pub const MAGIC: u32 = 0xD3ADB33F;
pub const Header = struct {
    // Should be the checksum of everything in the page after it
    magic: u32 = 0,
    crc32: u32 = 0,
    pageID: u32 = 0,
    lsn: u32 = 0,
    pageType: Type = .free,
};

pub const ControlBlock = struct {
    live: bool = false,
    lastAccess: usize = 0,
    pins: u64 = 0,
    dirty: bool = false,
    latch: Latch = .{},
    buffer: [PAGE_SIZE]u8 align(PAGE_SIZE),

    const Self = @This();

    pub fn init(self: *Self) !void {
        self.live = false;
        self.pins = 0;
        self.dirty = false;
        self.lastAccess = 0;
        self.latch = .{};
    }

    pub fn reinit(self: *Self, access: usize) void {
        self.dirty = false;
        self.live = true;
        self.pins = 0;
        self.latch = .{};
        self.lastAccess = access;
    }

    pub fn deinit(self: *Self) !void {
        assert(!self.dirty);
        log.debug("deinit page={d}", .{self.id()});
        //mem.free(self.buffer);
        self.* = undefined;
    }

    pub fn header(self: *Self) *Header {
        return @ptrCast(*Header, @alignCast(@alignOf(Header), self.buffer[0..]));
    }

    pub fn id(self: *Self) u32 {
        return self.header().pageID;
    }

    pub fn pinned(self: *Self) bool {
        return @atomicLoad(u64, &self.pins, .Acquire) > 0;
    }

    pub fn pin(self: *Self) void {
        _ = @atomicRmw(u64, &self.pins, .Add, 1, .Acquire);
    }

    pub fn unpin(self: *Self) void {
        assert(self.pins >= 0);
        _ = @atomicRmw(u64, &self.pins, .Sub, 1, .Release);
    }

    // pub fn hash(self: *Self) u32 {
    //     return Crc32.hash(std.mem.asBytes(self)[@offsetOf(Self, "crc32") + @sizeOf(u32) ..]);
    // }
};
