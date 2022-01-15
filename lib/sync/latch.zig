const std = @import("std");
const maxInt = @import("std").math.maxInt;

const State = u64;

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
            _ = @atomicRmw(u64, &self.latch.holds, .Sub, self.shares, .Release);
            self.latch = undefined;
        }
    };

    pub fn init(mem: std.mem.Allocator) !*Self {
        var l = try mem.create(Self);
        l.holds = 0;
        return l;
    }

    pub fn shared(self: *Self) Hold {
        while (true) {
            const holds = @atomicLoad(State, &self.holds, .Unordered);
            if (holds < maxInt(State)) {
                _ = @cmpxchgWeak(State, &self.holds, holds, holds + 1, .Acquire, .Acquire) orelse {
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
const testAllocator = std.testing.allocator;
const expectEqual = std.testing.expectEqual;

test "latches can be share locked and unlocked" {
    var latch = try Latch.init(testAllocator);
    defer testAllocator.destroy(latch);

    var hold = latch.shared();
    try expectEqual(latch.holds, 1);
    hold.release();
    try expectEqual(latch.holds, 0);
}

test "latches can be exclusively locked and unlocked" {
    var latch = try Latch.init(testAllocator);
    defer testAllocator.destroy(latch);

    var hold = latch.exclusive();
    try expectEqual(latch.holds, maxInt(State));
    hold.release();
    try expectEqual(latch.holds, 0);
}
