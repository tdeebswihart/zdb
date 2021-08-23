const std = @import("std");
const storage = @import("storage.zig");

fn openFile(fpath: []const u8) !std.fs.File {
    const cwd = std.fs.cwd();
    return cwd.openFile(fpath, .{ .read = true, .write = true }) catch |err| switch (err) {
        error.FileNotFound => try cwd.createFile(fpath, .{ .read = true, .mode = 0o755 }),
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
    var fs = storage.File.init(dbfile);
    const mgr = try storage.Manager.init(&fs.store, 4096, 4096 * 5, allocator);
    defer {
        mgr.deinit() catch |err| {
            std.debug.print("failed to deinit manager: {any}\n", .{err});
        };
    }

    var pin = try mgr.pin(0);
    defer pin.deinit();
    var sharedPage = pin.shared();

    const b1: []const u8 = &[_]u8{ 0x41, 0x42, 0x43 };
    const bytes = sharedPage.get(0) catch |_| {
        sharedPage.deinit();
        var xPage = pin.exclusive();
        const e2 = try xPage.put(b1);
        std.debug.print("wrote {any} to ({d},{d})\n", .{ e2, e2.page, e2.slot });
        return;
    };
    if (!std.mem.eql(u8, bytes, b1)) {
        std.debug.print("read {any} but expected {any}\n", .{ bytes, b1 });
    } else {
        std.debug.print("read {any}\n", .{bytes});
    }
    sharedPage.deinit();
}
