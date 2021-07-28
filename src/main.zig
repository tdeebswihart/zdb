const std = @import("std");
const storage = @import("storage.zig");

fn openFile(fpath: []const u8) !std.fs.File {
    const cwd = std.fs.cwd();
    return cwd.openFile(fpath, .{ .read = true, .write = true }) catch |err| switch (err) {
        error.FileNotFound => try cwd.createFile(fpath, .{.read = true, .mode = 0o755}),
        else => err,
    };
}

pub fn main() !void {
    const stderr = std.io.getStdErr();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = &gpa.allocator;
    defer {
        const leaked = gpa.deinit();
        if (leaked) stderr.writeAll("leaked memory\n") catch @panic("failed to write to stderr");
    }
    const dbfile = try openFile("init.zdb");
    defer dbfile.close();
    const mgr = try storage.FileManager.init(.{.context = dbfile}, 4096, allocator);
    defer {
        mgr.deinit() catch |err| {
            std.debug.print("failed to deinit manager: {any}\n", .{err});
        };
    }
    const b1: []const u8 = &[_]u8{0x41, 0x42, 0x43};
    const entry = try mgr.put(b1);
    const read = try mgr.get(entry);
    defer allocator.free(read);
    if (!std.mem.eql(u8, read, b1)) {
        std.debug.print("read {any} but expected {any}\n", .{read, b1});
    } else {
        std.debug.print("read {any} at ({d},{d})\n", .{read, entry.block, entry.slot});
    }

}
