const std = @import("std");
const maxInt = @import("std").math.maxInt;

const State = u64;
const log = std.log.scoped(.latch);

pub const Latch = struct {
    holds: State = 0,

    const Self = @This();

    pub const Kind = enum { shared, exclusive };
    /// Holds are not valid for concurrent use
    pub const Hold = struct {
        shares: State,
        latch: *Latch,

        /// Holds are no longer valid after calling release.
        pub fn release(self: *@This()) void {
            _ = @atomicRmw(State, &self.latch.holds, .Sub, self.shares, .Release);
            log.debug("shares of {*}={d}", .{ self.latch, self.latch.holds });
            self.* = undefined;
        }
    };

    pub fn init(mem: std.mem.Allocator) *Latch {
        return mem.create(Latch) catch |err| {
            std.debug.panic("failed to allocate latch: {s}", .{err});
        };
    }

    pub inline fn deinit(self: *Self, mem: std.mem.Allocator) void {
        mem.destroy(self);
    }

    pub fn shared(self: *Self) Hold {
        while (true) {
            const holds = @atomicLoad(State, &self.holds, .Unordered);
            log.debug("before grab {*}={d}", .{ self, self.holds });
            if (holds < maxInt(State)) {
                _ = @cmpxchgWeak(State, &self.holds, holds, holds + 1, .Acquire, .Acquire) orelse {
                    log.debug("shares of {*}={d}", .{ self, self.holds });
                    return Hold{
                        .shares = 1,
                        .latch = self,
                    };
                };
            }
            // exclusively locked
            while (@atomicLoad(State, &self.holds, .Unordered) == maxInt(State)) {}
        }
    }

    pub fn exclusive(self: *Self) Hold {
        while (true) {
            const holds = @atomicLoad(State, &self.holds, .Unordered);
            if (holds == 0) {
                _ = @cmpxchgWeak(State, &self.holds, 0, maxInt(State), .Acquire, .Acquire) orelse {
                    log.debug("shares of {*}={d}", .{ self, self.holds });
                    return Hold{
                        .shares = maxInt(State),
                        .latch = self,
                    };
                };
            }
            // locked by someone
            while (@atomicLoad(State, &self.holds, .Unordered) > 0) {}
        }
    }
};

const Thread = std.Thread;
const expectEqual = std.testing.expectEqual;

test "latches can be share locked and unlocked" {
    var latch = .{};

    var hold = latch.shared();
    try expectEqual(latch.holds, 1);
    hold.release();
    try expectEqual(latch.holds, 0);
}

test "multiple shared holds can be taken on a latch" {
    var latch = .{};

    var hold = latch.shared();
    try expectEqual(latch.holds, 1);
    var hold2 = latch.shared();
    try expectEqual(latch.holds, 2);
    hold.release();
    try expectEqual(latch.holds, 1);
    hold2.release();
    try expectEqual(latch.holds, 0);
}

test "latches can be exclusively locked and unlocked" {
    var latch = .{};

    var hold = latch.exclusive();
    try expectEqual(latch.holds, maxInt(State));
    hold.release();
    try expectEqual(latch.holds, 0);
}
