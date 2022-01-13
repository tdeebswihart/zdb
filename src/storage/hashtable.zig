const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;
const math = std.math;
const Page = @import("page.zig").Page;
const Latch = @import("libdb").sync.Latch;
const XXHash = @import("libdb").XXHash;
const PAGE_SIZE = @import("config.zig").PAGE_SIZE;
const BufferManager = @import("buffer.zig").Manager;
const PageDirectory = @import("page_directory.zig").Directory;

pub const DirectoryPage = struct {
    pageID: u32,
    /// Log sequence number
    lsn: u32,
    globalDepth: u32,
    localDepths: [512]u8,
    bucketPageIDs: [512]u32,

    const Self = @This();

    pub fn init(page: *Page) *Self {
        var self = @ptrCast(*Self, @alignCast(@alignOf(Self), page.buffer));
        if (self.pageID != page.id) {
            self.lsn = 0;
            self.globalDepth = 0;
            for (self.localDepths) |*b| b.* = 1;
            for (self.bucketPageIDs) |*b| b.* = 0;
        }
        return self;
    }
};

/// The amount of data a BucketPage can store is based on the key, value,
/// and page sizes.
pub fn BucketPage(comptime K: type, comptime V: type) type {
    const Entry = struct { key: K, val: V };
    return struct {
        pub const maxEntries = 4 * PAGE_SIZE / (4 * @sizeOf(Entry) + 1);
        pageID: u64,
        occupied: [maxEntries]u1,
        // 0 if tombstoned or unoccupied
        // 1 otherwise
        readable: [maxEntries]u1,
        data: [maxEntries]Entry,
        const Self = @This();

        pub fn init(page: *Page) *Self {
            var self = @ptrCast(*Self, @alignCast(@alignOf(Self), page.buffer));
            if (self.pageID != page.id) {
                for (self.occupied) |*b| b.* = 0;
                for (self.readable) |*b| b.* = 0;
            }
            return self;
        }

        pub fn get(self: *Self, i: u16) ?Entry {
            assert(i < maxEntries);
            if (self.readable[i] == 0) {
                return null;
            }
            return self.data[i];
        }

        pub fn put(self: *Self, i: u16, key: K, val: V) bool {
            if (self.readable[i] == 1) {
                return false;
            }
            self.occupied[i] = 1;
            self.readable[i] = 1;
            self.data[i] = .{ .key = key, .val = val };
            return true;
        }

        pub fn remove(self: *Self, i: u16, key: K) void {
            if (self.data[i].key == key) {
                self.readable[i] = 0;
            }
        }

        pub fn forceRemove(self: *Self, i: u16) void {
            self.readable[i] = 0;
        }
    };
}

pub fn HashTable(comptime K: type, comptime V: type) type {
    const Bucket = BucketPage(K, V);
    const ArrayList = std.ArrayList(V);
    return struct {
        seed: u32,
        dirPage: *Page,
        directory: *DirectoryPage,
        bm: *BufferManager,
        pageDir: PageDirectory,
        latch: *Latch,
        mem: std.mem.Allocator,

        const Self = @This();

        pub fn new(alloc: std.mem.Allocator, bm: *BufferManager, pageDir: PageDirectory) !Self {
            var ht = try Self.init(alloc, bm, pageDir, try pageDir.allocate());
            errdefer ht.destroy() catch |e| {
                std.debug.panic("failed to destroy hashtable: {?}", .{e});
            };

            const dir = ht.directory;
            var dirhold = ht.dirPage.latch.exclusive();
            defer dirhold.release();

            var b1p = try pageDir.allocate();
            defer b1p.unpin();
            var b1l = b1p.latch.exclusive();
            defer b1l.release();
            _ = Bucket.init(b1p);

            var b2p = try pageDir.allocate();
            defer b2p.unpin();
            var b2l = b2p.latch.exclusive();
            defer b2l.release();
            _ = Bucket.init(b2p);

            dir.globalDepth = 1;
            // set up our first two buckets
            dir.localDepths[0] = 1;
            dir.localDepths[1] = 1;
            dir.bucketPageIDs[0] = b1p.id;
            dir.bucketPageIDs[1] = b2p.id;

            b1p.dirty = true;
            b2p.dirty = true;
            ht.dirPage.dirty = true;
            return ht;
        }

        pub fn init(alloc: std.mem.Allocator, bm: *BufferManager, pageDir: PageDirectory, dirPage: *Page) !Self {
            return Self{
                .seed = 0,
                .dirPage = dirPage,
                .directory = DirectoryPage.init(dirPage),
                .bm = bm,
                .pageDir = pageDir,
                .latch = try Latch.init(alloc),
                .mem = alloc,
            };
        }

        /// Destroy this hashtable. Do not call alongside deinit
        pub fn destroy(self: *Self) !void {
            var h = self.latch.exclusive();
            errdefer h.release();
            const pages = @as(u32, 2) << @intCast(u5, self.directory.globalDepth - 1);
            var idx: u16 = 0;
            while (idx < pages) : (idx += 1) {
                if (self.directory.bucketPageIDs[idx] == 0) {
                    // this should not occur
                    break;
                }
                try self.pageDir.free(self.directory.bucketPageIDs[idx]);
            }
            try self.pageDir.free(self.dirPage.id);
            self.dirPage.unpin();
            self.dirPage = undefined;
            self.directory = undefined;
            self.pageDir = undefined;
            h.release();
            self.mem.destroy(self.latch);
        }

        pub fn deinit(self: *Self) void {
            var h = self.latch.exclusive();
            self.dirPage.unpin();
            self.dirPage = undefined;
            self.directory = undefined;
            h.release();
            self.mem.destroy(self.latch);
        }

        inline fn checksum(self: Self, key: K) u64 {
            return XXHash.checksum(mem.asBytes(&key), self.seed);
        }

        inline fn prefix(self: Self, hsh: u64) u64 {
            const mask = ~@as(u64, 0) >> @intCast(u6, @bitSizeOf(u64) - self.directory.globalDepth);
            return hsh & mask;
        }

        pub fn get(self: Self, key: K, results: *ArrayList) !void {
            const hsh = self.checksum(key);
            const pfx = self.prefix(hsh);
            var hold = self.latch.shared();
            defer hold.release();

            const bp = try self.bm.pin(self.directory.bucketPageIDs[pfx]);
            defer bp.unpin();
            var bh = bp.latch.shared();
            defer bh.release();

            var b = Bucket.init(bp);
            var i: u16 = 0;
            while (b.occupied[i] == 1 and i < Bucket.maxEntries) {
                if (b.get(@intCast(u16, i))) |entry| {
                    try results.append(entry.val);
                }
                i += 1;
            }
        }

        pub fn put(self: Self, key: K, val: V) anyerror!bool {
            const hsh = self.checksum(key);
            var idx = self.prefix(hsh);

            var hold = self.latch.exclusive();
            defer hold.release();

            const d = self.directory;
            const bp = try self.bm.pin(d.bucketPageIDs[idx]);
            defer bp.unpin();
            var bh = bp.latch.exclusive();
            defer bh.release();

            var b = Bucket.init(bp);
            var i: u16 = 0;
            while (i < Bucket.maxEntries) : (i += 1) {
                if (b.put(i, key, val)) {
                    return true;
                }
            }

            const mirror = try self.pageDir.allocate();
            defer mirror.unpin();
            var mh = mirror.latch.exclusive();
            defer mh.release();
            var mb = Bucket.init(mirror);

            d.localDepths[idx] += 1;
            const newIdx = idx << 1;
            const mirrorIdx = idx + 1;
            if (d.localDepths[idx] > d.globalDepth) {
                // Double in size
                // Starting from the last active page, each gets
                // (idx << 1) and (idx << 1 + 1)
                // We then overwrite mirrorIdx with mirror's pageId
                var last = (@as(u32, 2) << @intCast(u5, d.globalDepth)) - 1;
                // The bucket at idx 0 never changes
                while (last > 0) : (last -= 1) {
                    d.bucketPageIDs[last << 1] = d.bucketPageIDs[last];
                    d.localDepths[last << 1] = d.localDepths[last];
                    d.bucketPageIDs[last << 1 + 1] = d.bucketPageIDs[last];
                    d.localDepths[last << 1 + 1] = d.localDepths[last];
                }
                d.bucketPageIDs[mirrorIdx] = mirror.id;
                d.globalDepth += 1;
                // Recalculate insertion idx
                idx = self.prefix(hsh);
            } else {
                // Each of page, mirror should occupy 2**(global depth - old local dopth - 1) spaces
                const toOccupy = @as(u32, 2) << @intCast(u5, d.globalDepth - d.localDepths[idx] - 2);
                var taken: u32 = 0;
                while (taken < toOccupy) : (taken += 1) {
                    d.localDepths[newIdx + taken] = d.localDepths[idx];
                    d.bucketPageIDs[newIdx + taken] = bp.id;
                }
                taken = 0;
                while (taken < toOccupy) : (taken += 1) {
                    d.localDepths[mirrorIdx + taken] = d.localDepths[idx];
                    d.bucketPageIDs[mirrorIdx + taken] = mirror.id;
                }
            }

            var mirrorOffset: u16 = 0;
            var baseFirstAvailable = @intCast(u16, b.readable.len);
            i = 0;
            while (i < Bucket.maxEntries and b.occupied[i] == 1) : (i += 1) {
                if (b.readable[i] == 1) {
                    const e = b.data[i];
                    const ehsh = self.checksum(e.key);
                    if (self.prefix(ehsh) == mirrorIdx) {
                        assert(mb.put(mirrorOffset, e.key, e.val));
                        mirrorOffset += 1;
                        b.forceRemove(i);
                        baseFirstAvailable = i;
                    }
                }
            }

            if (idx == mirrorIdx) {
                return mb.put(mirrorOffset, key, val);
            } else {
                return try self.put(key, val);
            }
        }

        /// FIXME: merge pages if their load is below a certain amount
        pub fn remove(self: Self, key: K) ?V {
            const hsh = self.checksum(key);
            const pfx = self.prefix(hsh);
            var hold = self.latch.shared();
            defer hold.release();

            const bp = try self.bm.pin(self.directory.bucketPageIDs[pfx]);
            defer bp.unpin();
            var bh = bp.latch.exclusive();
            defer bh.release();

            var b = Bucket.init(bp);
            var i: u16 = 0;
            while (b.occupied[i] == 1 and i < Bucket.maxEntries) {
                b.remove(i, key);
                i += 1;
            }
        }
    };
}
