const std = @import("std");
const storage = @import("storage.zig");
const tuple = storage.tuple;

const log = std.log.scoped(.zdb);

fn openFile(fpath: []const u8) !std.fs.File {
    const cwd = std.fs.cwd();
    return cwd.openFile(fpath, .{ .read = true, .write = true }) catch |err| switch (err) {
        error.FileNotFound => try cwd.createFile(fpath, .{ .read = true, .mode = 0o755 }),
        else => err,
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const leaked = gpa.deinit();
        if (leaked) log.err("leaked memory", .{});
    }
    const dbfile = try openFile("init.zdb");
    var fs = try storage.File.init(allocator, dbfile);
    var mgr = fs.manager();
    defer mgr.deinit();
    const bm = try storage.BufferManager.init(fs.manager(), 50, allocator);
    defer {
        bm.deinit() catch |err| {
            log.err("failed to deinit manager: {any}\n", .{err});
        };
    }

    var pd = try storage.PageDirectory.init(allocator, bm);
    defer pd.deinit();

    var p1 = try pd.allocLatched(.exclusive);
    const pageID = p1.page.id;
    p1.deinit();
    try pd.free(pageID);

    // We should get the same page back
    var p2 = try pd.allocLatched(.exclusive);
    if (pageID != p2.page.id) {
        log.err("expected={d} got={d}", .{ pageID, p2.page.id });
        return;
    }
    const p2ID = p2.page.id;
    p2.deinit();
    log.info("deinit p2", .{});
    try pd.free(p2ID);
    log.info("hashtable time", .{});

    var ht = try storage.HashTable(u16, u16).new(allocator, bm, pd);
    defer {
        ht.destroy() catch |err| {
            log.err("failed to destroy hash table: {any}", .{err});
        };
    }

    var i: u16 = 0;
    // We start with 2 pages of 512.
    while (i < 1024) : (i += 1) {
        if (!try ht.put(i, i)) {
            log.err("put={d} failed=true", .{i});
        }
    }

    var results = std.ArrayList(u16).init(allocator);
    defer results.deinit();
    i = 0;
    while (i < 1024) : (i += 1) {
        results.clearRetainingCapacity();
        try ht.get(i, &results);
        const expected = &[_]u16{i};
        if (!std.mem.eql(u16, expected, results.items)) {
            log.err("i={d} expected={any} got={any}", .{ i, expected, results.items });
        }
    }
}
