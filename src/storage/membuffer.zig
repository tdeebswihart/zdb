const std = @import("std");
const File = @import("file.zig").File;

const ByteList = std.ArrayList(u8);

const Error = error{
    SeekTooLarge,
};

const ByteArray = struct {
    data: std.ArrayList(u8),
    head: usize = 0,

    const Self = @This();

    pub fn init(sz: usize, mem: std.mem.Allocator) !Self {
        var arr = try std.ArrayList(u8).initCapacity(mem, sz);
        try arr.resize(sz);
        return Self{
            .data = arr,
        };
    }

    pub fn deinit(self: Self) void {
        self.data.deinit();
    }
    // Mostly for testing purposes
    pub fn initWithBuffer(buffer: []u8, mem: std.mem.Allocator) Self {
        return Self{
            .data = std.ArrayList(u8).fromOwnedSlice(mem, buffer),
        };
    }

    pub fn read(self: *Self, buffer: []u8) !usize {
        std.debug.assert(self.head <= self.data.items.len);
        const toCopy = self.data.items.len - self.head;
        _ = std.mem.copy(u8, buffer[0..toCopy], self.data.items[self.head..(self.head + toCopy)]);
        self.head += toCopy;
        return toCopy;
    }

    pub fn write(self: *Self, buffer: []const u8) !usize {
        std.debug.assert(self.head < self.data.capacity);
        const len = buffer.len;
        if (buffer.len + self.head > self.data.capacity) {
            try self.data.resize(buffer.len + self.head);
        }
        _ = std.mem.copy(u8, self.data.items[self.head..(self.head + len)], buffer);
        self.head += len;
        return len;
    }

    pub fn seekTo(self: *Self, pos: usize) !void {
        if (pos > self.data.items.len) {
            return Error.SeekTooLarge;
        }
        self.head = pos;
    }

    pub fn extend(self: *Self, sz: usize) !void {
        self.data.resize(sz);
    }

    pub fn size(self: Self) !u64 {
        return self.data.items.len;
    }
};

pub const Buffer = File(
    ByteArray,
    ByteArray.read,
    anyerror,
    ByteArray.write,
    anyerror,
    ByteArray.seekTo,
    anyerror,
    ByteArray.extend,
    anyerror,
    ByteArray.size,
    anyerror,
);

// *** Testing ***
const testing = std.testing;
const expect = testing.expect;
test "ByteArrays can be read from" {
    const buf = try testing.allocator.alloc(u8, 3);
    buf[0] = 0x41;
    buf[1] = 0x42;
    buf[2] = 0x43;
    var arr = ByteArray.initWithBuffer(buf, testing.allocator);
    defer arr.deinit();

    const buf2 = try testing.allocator.alloc(u8, 3);
    defer testing.allocator.free(buf2);
    try testing.expectEqual(@intCast(usize, 3), try arr.read(buf2));

    const expected: []const u8 = &[_]u8{ 0x41, 0x42, 0x43 };
    try testing.expectEqualSlices(u8, expected, buf2);
    try testing.expectEqual(@intCast(usize, 0), try arr.read(buf2));
}

test "ByteArrays - write, seek, read" {
    var arr = try ByteArray.init(3, testing.allocator);
    defer arr.deinit();
    const written = try arr.write(&[_]u8{ 0x41, 0x42, 0x43 });
    try expect(written == 3);
    // should be at EOF
    const buf = try testing.allocator.alloc(u8, 3);
    defer testing.allocator.free(buf);
    try testing.expectEqual(@intCast(usize, 0), try arr.read(buf));

    // seek to beginning
    try arr.seekTo(0);
    try testing.expectEqual(@intCast(usize, 3), try arr.read(buf));
}

test "extending ByteArrays" {
    var arr = try ByteArray.init(3, testing.allocator);
    defer arr.deinit();
    const written = try arr.write(&[_]u8{ 0x41, 0x42, 0x43, 0x44, 0x45 });
    try expect(written == 5);
}
