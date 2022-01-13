const std = @import("std");
const t = std.testing;
const BufferManager = @import("buffer.zig").Manager;
const tuple = @import("tuple.zig");
const FileManager = @import("file.zig").Manager;
const File = @import("file.zig").File;
const allocPrint = std.fmt.allocPrint;
const alloc = t.allocator;
const panic = std.debug.panic;

pub const Test = struct {
    runid: usize,
    path: []const u8,
    rng: std.rand.Random,
    fs: *File,
    mgr: FileManager,
    bm: *BufferManager,

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
        return @This(){ .runid = id, .path = path, .rng = prng, .fs = fs, .mgr = mgr, .bm = bm };
    }

    pub fn teardownKeepData(self: *@This()) !void {
        defer self.mgr.deinit();
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

test "pinned pages are not evicted when space is needed" {
    var ctx = try Test.setup(1);
    defer ctx.teardown();

    var page = try ctx.bm.pin(0);
    try t.expectError(BufferManager.Error.Full, ctx.bm.pin(1));
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
