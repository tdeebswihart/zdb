const std = @import("std");
const t = std.testing;
const BufferManager = @import("buffer.zig").Manager;
const tuple = @import("tuple.zig");
const FileManager = @import("file.zig").Manager;
const PageDirectory = @import("page_directory.zig").Directory;
const File = @import("file.zig").File;
const allocPrint = std.fmt.allocPrint;
const alloc = t.allocator;
const panic = std.debug.panic;
const expect = t.expect;

pub const Test = struct {
    runid: usize,
    path: []const u8,
    rng: std.rand.Random,
    fs: *File,
    mgr: FileManager,
    bm: *BufferManager,
    pd: PageDirectory,

    pub fn setup(nPages: usize) !@This() {
        var prng = std.rand.DefaultPrng.init(0).random();
        const id = prng.int(usize);
        const path = try allocPrint(alloc, "/tmp/{d}.zdb", .{id});
        const f = try std.fs.createFileAbsolute(path, .{
            .read = true,
            .truncate = true,
        });
        errdefer {
            f.close();
            std.fs.deleteFileAbsolute(path) catch |e| panic("failed to delete {s}: {s}", .{ path, e });
        }
        var fs = try File.init(alloc, f);
        var mgr = fs.manager();
        errdefer mgr.deinit();
        const bm = try BufferManager.init(mgr, nPages, alloc);
        var pd = try PageDirectory.init(alloc, bm);
        return @This(){ .runid = id, .path = path, .rng = prng, .fs = fs, .mgr = mgr, .bm = bm, .pd = pd };
    }

    pub fn teardownKeepData(self: *@This()) !void {
        defer self.mgr.deinit();
        self.pd.deinit();
        try self.bm.deinit();
    }

    pub fn teardown(self: *@This()) void {
        defer alloc.free(self.path);
        const err = self.teardownKeepData();
        const err2 = std.fs.deleteFileAbsolute(self.path);
        err catch |e| {
            panic("failed to tear down: {s}", .{e});
        };
        err2 catch |e| {
            panic("failed to delete file {s}: {s}", .{ self.path, e });
        };
    }
};

// *** Paging ***
test "pinned pages are not evicted when space is needed" {
    var ctx = try Test.setup(1);
    defer ctx.teardown();

    var page = try ctx.bm.pin(0);
    try t.expectError(BufferManager.Error.Full, ctx.bm.pin(1));
    page.unpin();
}

test "unpinned pages are evicted when space is needed" {
    var ctx = try Test.setup(2);
    defer ctx.teardown();

    var page = try ctx.bm.pin(0);
    page.unpin();
    page = try ctx.bm.pin(1);
    page.unpin();
}

test "tuple pages can be written to" {
    var ctx = try Test.setup(5);
    defer ctx.teardown();

    const expected: []const u8 = &[_]u8{ 0x41, 0x42, 0x43 };
    var page = try ctx.bm.pin(0);

    var xPage = try tuple.Writable.init(page);
    const loc = try xPage.put(expected);
    xPage.deinit();
    var shared = try tuple.Readable.init(page);
    const found = try shared.get(loc.slot);
    try t.expectEqualSlices(u8, found, expected);
    shared.deinit();
    page.unpin();
}

// *** Hash Table ***
const HashTable = @import("hashtable.zig").HashTable;
test "new hash tables can be created" {
    var ctx = try Test.setup(10);
    defer ctx.teardown();

    var ht = try HashTable(u16, u16).new(alloc, ctx.bm, ctx.pd);
    try ht.destroy();
}

test "hashtables can store and retrieve values" {
    var ctx = try Test.setup(10);
    defer ctx.teardown();

    var ht = try HashTable(u16, u16).new(alloc, ctx.bm, ctx.pd);
    defer ht.destroy() catch |e| panic("{s}", .{e});

    try expect(try ht.put(0, 1));
    try expect(try ht.put(0, 2));
    var results = std.ArrayList(u16).init(alloc);
    defer results.deinit();
    try ht.get(0, &results);
    try t.expectEqualSlices(u16, &[_]u16{ 1, 2 }, results.items);
}

test "hashtable values can be removed" {
    var ctx = try Test.setup(10);
    defer ctx.teardown();

    var ht = try HashTable(u16, u16).new(alloc, ctx.bm, ctx.pd);
    defer ht.destroy() catch |e| panic("{s}", .{e});

    try expect(try ht.put(0, 1));
    try expect(try ht.put(0, 2));
    try ht.remove(0, 1);
    var results = std.ArrayList(u16).init(alloc);
    defer results.deinit();
    try ht.get(0, &results);
    try t.expectEqualSlices(u16, &[_]u16{2}, results.items);
}

test "hashtables can handle array-based keys" {
    var ctx = try Test.setup(10);
    defer ctx.teardown();

    var ht = try HashTable([255:0]u8, u16).new(alloc, ctx.bm, ctx.pd);
    defer ht.destroy() catch |e| panic("{s}", .{e});

    var buf: [255:0]u8 = std.mem.zeroes([255:0]u8);
    buf[0] = 'h';
    buf[1] = 'e';
    buf[2] = 'l';
    buf[3] = 'l';
    buf[4] = 'o';

    try expect(try ht.put(buf, 1));
    try expect(try ht.put(buf, 2));
    var results = std.ArrayList(u16).init(alloc);
    defer results.deinit();
    try ht.get(buf, &results);
    try t.expectEqualSlices(u16, &[_]u16{ 1, 2 }, results.items);
}

test "hashtables can split pages" {
    var ctx = try Test.setup(50);
    defer ctx.teardown();

    var ht = try HashTable(u16, u16).new(alloc, ctx.bm, ctx.pd);
    defer ht.destroy() catch |e| panic("{s}", .{e});

    var i: u16 = 0;
    while (i < 1024) : (i += 1) {
        try expect(try ht.put(i, i));
    }

    var results = std.ArrayList(u16).init(alloc);
    defer results.deinit();
    i = 0;
    while (i < 1024) : (i += 1) {
        results.clearRetainingCapacity();
        try ht.get(i, &results);
        try t.expectEqualSlices(u16, &[_]u16{i}, results.items);
    }
}
