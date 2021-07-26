const std = @import("std");
const storage = @import("storage/storage.zig");

pub fn main() !void {
    const stderr = std.io.getStdErr();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = &gpa.allocator;
    defer {
        const leaked = gpa.deinit();
        if (leaked) stderr.writeAll("leaked memory\n") catch @panic("failed to write to stderr");
    }
    const dbfile = try std.fs.cwd().createFile("init.zdb", .{ .read = true, .truncate = true, .mode = 0o755 });
    defer dbfile.close();
    const mgr = try storage.Manager(storage.FSFile).init(.{.context = dbfile}, 4096, allocator);
    defer {
        mgr.deinit() catch |err| {
            std.debug.print("failed to deinit manager: {any}\n", .{err});
        };
    }
    _ = try mgr.put(&[_]u8{0x41, 0x42, 0x43});
}
