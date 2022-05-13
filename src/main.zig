const std = @import("std");
const storage = @import("storage.zig");
const tuple = storage.tuple;

const logr = std.log.scoped(.zdb);

fn openFile(fpath: []const u8) !std.fs.File {
    const cwd = std.fs.cwd();
    return cwd.openFile(fpath, .{ .mode = .read_write }) catch |err| switch (err) {
        error.FileNotFound => try cwd.createFile(fpath, .{ .read = true, .mode = 0o755 }),
        else => err,
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const leaked = gpa.deinit();
        if (leaked) logr.err("leaked memory", .{});
    }
    const dbfile = openFile("init.zdb") catch |err| {
        logr.err("failed to open database: {any}\n", .{err});
        return;
    };
    var fs = storage.File.init(allocator, dbfile) catch |err| {
        logr.err("failed to wrap db file: {any}", .{err});
        return;
    };
    var mgr = fs.manager();
    defer mgr.deinit();
    const bm = storage.BufferManager.init(fs.manager(), 50, allocator) catch |err| {
        logr.err("failed create bm: {any}\n", .{err});
        return;
    };
    defer {
        bm.deinit() catch |err| {
            logr.err("failed to deinit manager: {any}\n", .{err});
        };
    }

    var p1 = try bm.allocate(.tuple);
    _ = tuple.TuplePage.new(p1);
    var xPage = try tuple.Writable.init(p1);
    _ = try xPage.put(&[_]u8{ 0x41, 0x42, 0x43 });
    const pageID = p1.id();
    xPage.deinit();
    p1 = undefined;
    try bm.free(pageID);

    // We should get the same page back
    var p2 = try bm.allocLatched(.tuple, .exclusive);
    if (p2.page.id() != pageID) {
        logr.err("expected={d} found={d}", .{ pageID, p2.page.id() });
    }
    const p2ID = p2.page.id();
    p2.deinit();
    try bm.free(p2ID);

    var ht = try storage.HashTable(u16, u16).new(allocator, bm);
    defer {
        ht.destroy() catch |err| {
            logr.err("failed to destroy hash table: {any}", .{err});
        };
    }

    var i: u16 = 0;
    // We start with 2 pages of 512.
    while (i < 1024) : (i += 1) {
        if (!try ht.put(i, i)) {
            logr.err("put={d} failed=true", .{i});
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
            logr.err("i={d} expected={any} got={any}", .{ i, expected, results.items });
        }
    }
}

pub const log_level: std.log.Level = .debug;
// Define root.log to override the std implementation
pub fn log(
    comptime level: std.log.Level,
    comptime scope: @TypeOf(.EnumLiteral),
    comptime format: []const u8,
    args: anytype,
) void {
    // Ignore all non-error logging from sources other than
    // .my_project, .nice_library and .default
    const scope_prefix = switch (scope) {
        .hashtable => @tagName(scope),
        else => if (@enumToInt(level) <= @enumToInt(std.log.Level.err))
            @tagName(scope)
        else
            return,
    } ++ ": ";

    const prefix = "[" ++ level.asText() ++ "] " ++ scope_prefix;

    // Print the message to stderr, silently ignoring any errors
    std.debug.getStderrMutex().lock();
    defer std.debug.getStderrMutex().unlock();
    const stderr = std.io.getStdErr().writer();
    nosuspend stderr.print(prefix ++ format ++ "\n", args) catch return;
}
