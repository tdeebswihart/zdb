const std = @import("std");
const t = std.testing;
const BufferManager = @import("buffer.zig").Manager;
const tuple = @import("tuple.zig");
const FileManager = @import("file.zig").Manager;
const File = @import("file.zig").File;
const allocPrint = std.fmt.allocPrint;
const alloc = t.allocator;
const panic = std.debug.panic;
const expect = t.expect;

var rand = std.rand.DefaultPrng.init(42);
const prng = rand.random();

pub const Test = struct {
    runid: usize,
    path: []const u8,
    fs: *File,
    mgr: FileManager,
    bm: *BufferManager,

    pub fn setup(nPages: usize) !@This() {
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
        return @This(){ .runid = id, .path = path, .fs = fs, .mgr = mgr, .bm = bm };
    }

    pub fn teardownKeebmata(self: *@This()) !void {
        defer self.mgr.deinit();
        try self.bm.deinit();
    }

    pub fn teardown(self: *@This()) void {
        defer alloc.free(self.path);
        const err = self.teardownKeebmata();
        const err2 = std.fs.deleteFileAbsolute(self.path);
        err catch |e| {
            panic("failed to tear down: {s}", .{e});
        };
        err2 catch |e| {
            panic("failed to delete file {s}: {s}", .{ self.path, e });
        };
    }
};

// *** Buffer management ***
test "pinned pages are not evicted when space is needed" {
    var ctx = try Test.setup(1);
    defer ctx.teardown();

    try t.expectError(BufferManager.Error.Full, ctx.bm.pin(1, .free));
}

test "pages cannot be pinned for a mismatched type" {
    var ctx = try Test.setup(1);
    defer ctx.teardown();

    // Page 0 is the first page of the directory
    try t.expectError(BufferManager.Error.PageTypeMismatch, ctx.bm.pin(0, .tuple));
}

test "unpinned pages are evicted when space is needed" {
    var ctx = try Test.setup(2);
    defer ctx.teardown();

    var p = try ctx.bm.pin(1, .directory);
    p.unpin();

    p = try ctx.bm.pin(2, .directory);
    p.unpin();
}

test "pin and unpin many pages" {
    var ctx = try Test.setup(10);
    defer ctx.teardown();

    var i: u32 = 1;
    while (i < 256) : (i += 1) {
        var page = try ctx.bm.pin(i, .free);
        page.unpin();
    }
}

test "page types are validated when pinning" {
    var ctx = try Test.setup(10);
    defer ctx.teardown();
}

// *** Tuple pages ***
test "tuple pages can be written to" {
    var ctx = try Test.setup(5);
    defer ctx.teardown();

    const expected: []const u8 = &[_]u8{ 0x41, 0x42, 0x43 };
    var page = try ctx.bm.pin(1, .tuple);
    _ = tuple.TuplePage.new(page);

    var xPage = try tuple.Writable.init(page);
    const loc = try xPage.put(expected);
    xPage.deinit();
    var shared = try tuple.Readable.init(page);
    const found = try shared.get(loc.slot);
    try t.expectEqualSlices(u8, found, expected);
    shared.deinit();
    page.unpin();
}

// *** Page Directory ***
test "page directories can allocate and free pages" {
    var ctx = try Test.setup(5);
    defer ctx.teardown();
    var p1 = try ctx.bm.allocLatched(.directory, .exclusive);
    const pageID = p1.page.id();
    p1.deinit();
    try ctx.bm.free(pageID);

    // We should get the same page back
    var p2 = try ctx.bm.allocLatched(.tuple, .exclusive);
    try t.expectEqual(pageID, p2.page.id());
    const p2ID = p2.page.id();
    p2.deinit();
    try ctx.bm.free(p2ID);
}

test "page directories will add directory pages as needed" {
    // FIXME allocate more than PAGE_SIZE - 16 pages
    var ctx = try Test.setup(5);
    defer ctx.teardown();
}

// *** Hash Table ***
const HashTable = @import("hashtable.zig").HashTable;
test "new hash tables can be created" {
    var ctx = try Test.setup(10);
    defer ctx.teardown();

    var ht = try HashTable(u16, u16).new(alloc, ctx.bm);
    try ht.destroy();
}

test "hashtables can store and retrieve values" {
    var ctx = try Test.setup(10);
    defer ctx.teardown();

    var ht = try HashTable(u16, u16).new(alloc, ctx.bm);
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

    var ht = try HashTable(u16, u16).new(alloc, ctx.bm);
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

    var ht = try HashTable([255:0]u8, u16).new(alloc, ctx.bm);
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
    var ctx = try Test.setup(100);
    defer ctx.teardown();

    var ht = try HashTable(u16, u16).new(alloc, ctx.bm);
    defer ht.destroy() catch |e| panic("{s}", .{e});

    var i: u16 = 0;
    // We start with 2 pages of 512.
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
