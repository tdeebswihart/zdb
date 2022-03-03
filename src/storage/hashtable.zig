const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const assert = std.debug.assert;
const math = std.math;
const page = @import("page.zig");
const Latch = @import("libdb").sync.Latch;
const XXHash = @import("libdb").XXHash;
const PAGE_SIZE = @import("config.zig").PAGE_SIZE;
const BufferManager = @import("buffer.zig").Manager;

const log = std.log.scoped(.hashtable);

pub const DirectoryPage = struct {
    header: page.Header,
    globalDepth: u32,
    localDepths: [512]u8,
    bucketPageIDs: [512]u32,
    pageLoads: [512]u8,

    const Self = @This();

    pub fn init(p: *page.ControlBlock) *Self {
        return @ptrCast(*Self, @alignCast(@alignOf(Self), p.buffer[0..]));
    }

    pub fn new(p: *page.ControlBlock) *Self {
        var self = Self.init(p);
        self.globalDepth = 0;
        for (self.localDepths) |*b| b.* = 1;
        for (self.bucketPageIDs) |*b| b.* = 0;
        for (self.pageLoads) |*b| b.* = 0;
        self.pageID = p.id();
        return self;
    }
};

/// The amount of data a BucketPage can store is based on the key, value,
/// and page sizes.
pub fn BucketPage(comptime K: type, comptime V: type) type {
    const Entry = struct {
        key: K,
        val: V,
    };
    return struct {
        pub const maxEntries = 4 * PAGE_SIZE / (4 * @sizeOf(Entry) + 1);
        header: page.Header,

        occupied: [maxEntries]u1,
        // 0 if tombstoned or unoccupied
        // 1 otherwise
        readable: [maxEntries]u1,
        data: [maxEntries]Entry,
        const Self = @This();

        pub fn new(p: *page.ControlBlock) *Self {
            var self = @ptrCast(*Self, @alignCast(@alignOf(Self), p.buffer[0..]));
            for (self.occupied) |*b| b.* = 0;
            for (self.readable) |*b| b.* = 0;
            return self;
        }

        pub fn init(p: *page.ControlBlock) *Self {
            return @ptrCast(*Self, @alignCast(@alignOf(Self), p.buffer[0..]));
        }

        pub fn get(self: *Self, i: u16, key: K) ?V {
            if (self.readable[i] == 0) {
                return null;
            }
            const e = &self.data[i];
            if (meta.eql(key, e.key)) {
                return e.val;
            }
            return null;
        }

        pub fn insert(self: *Self, initIdx: u16, key: K, val: V) bool {
            if (self.put(initIdx, key, val)) {
                return true;
            }
            // Wrap around until we find a space
            var i = initIdx + 1;
            while (i != initIdx) : (i += 1) {
                if (i > Self.maxEntries) {
                    i = 0;
                }
                if (self.put(i, key, val)) {
                    return true;
                }
            }
            return false;
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

        pub fn remove(self: *Self, i: u16, key: K, val: V) void {
            var e = &self.data[i];
            if (meta.eql(key, e.key) and meta.eql(val, e.val)) {
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
        dirPage: *page.ControlBlock,
        directory: *DirectoryPage,
        bm: *BufferManager,
        latch: Latch = .{},
        mem: std.mem.Allocator,

        const Self = @This();

        pub fn new(alloc: std.mem.Allocator, bm: *BufferManager) !*Self {
            const ht = try Self.init(alloc, bm, try bm.allocate(.hashDirectory));
            errdefer ht.destroy() catch |e| {
                std.debug.panic("failed to destroy hashtable: {?}", .{e});
            };

            const dir = ht.directory;
            var dirhold = ht.dirPage.latch.exclusive();
            defer dirhold.release();

            var b1 = try bm.allocLatched(.hashBucket, .exclusive);
            defer b1.deinit();
            _ = Bucket.new(b1.page);
            log.debug("bucket={d}", .{b1.page.id()});

            var b2 = try bm.allocLatched(.hashBucket, .exclusive);
            defer b2.deinit();
            _ = Bucket.new(b2.page);
            log.debug("bucket={d}", .{b2.page.id()});

            dir.globalDepth = 1;
            // set up our first two buckets
            dir.localDepths[0] = 1;
            dir.localDepths[1] = 1;
            dir.bucketPageIDs[0] = b1.page.id();
            dir.bucketPageIDs[1] = b2.page.id();

            b1.page.dirty = true;
            b2.page.dirty = true;
            ht.dirPage.dirty = true;

            return ht;
        }

        pub fn init(alloc: std.mem.Allocator, bm: *BufferManager, dirPage: *page.ControlBlock) !*Self {
            const ht = try alloc.create(Self);
            ht.seed = 0;
            ht.dirPage = dirPage;
            ht.latch = .{};
            ht.directory = DirectoryPage.init(dirPage);
            ht.bm = bm;
            ht.bm = bm;
            ht.mem = alloc;
            return ht;
        }

        /// Destroy this hashtable. Do not call alongside deinit
        pub fn destroy(self: *Self) !void {
            var h = self.latch.exclusive();
            errdefer h.release();
            if (self.directory.globalDepth > 0) {
                const pages = @as(u32, 2) << @intCast(u5, self.directory.globalDepth - 1);
                var idx: u16 = 0;
                while (idx < pages) : (idx += 1) {
                    if (self.directory.bucketPageIDs[idx] == 0) {
                        // this should not occur
                        break;
                    }
                    try self.bm.free(self.directory.bucketPageIDs[idx]);
                }
            }
            try self.bm.free(self.dirPage.id());
            self.dirPage.unpin();
            self.dirPage = undefined;
            self.directory = undefined;
            self.bm = undefined;
            self.mem.destroy(self);
        }

        pub fn deinit(self: *Self) void {
            _ = self.latch.exclusive();
            self.dirPage.unpin();
            self.dirPage = undefined;
            self.directory = undefined;
            self.* = undefined;
        }

        inline fn checksum(self: Self, key: K) u64 {
            return XXHash.checksum(mem.asBytes(&key), self.seed);
        }

        inline fn prefix(self: Self, hsh: u64) u64 {
            const mask = ~@as(u64, 0) >> @intCast(u6, @bitSizeOf(u64) - self.directory.globalDepth);
            return hsh & mask;
        }

        inline fn localIndex(self: Self, hsh: u64) u16 {
            // idc if we're truncating here
            return @intCast(u16, 0xFFFF & (hsh >> @intCast(u6, self.directory.globalDepth))) % Bucket.maxEntries;
        }

        pub fn get(self: *Self, key: K, results: *ArrayList) !void {
            const hsh = self.checksum(key);
            const pfx = self.prefix(hsh);
            var hold = self.latch.shared();
            defer hold.release();

            var bh = try self.bm.pinLatched(self.directory.bucketPageIDs[pfx], .hashBucket, .shared);
            defer bh.deinit();

            var b = Bucket.init(bh.page);
            var localIdx = self.localIndex(hsh);
            if (b.get(@intCast(u16, localIdx), key)) |val| {
                try results.append(val);
            }

            var i = localIdx + 1;
            while (i != localIdx and b.occupied[i] == 1) : (i += 1) {
                if (i > Bucket.maxEntries) {
                    i = 0;
                }
                if (b.get(@intCast(u16, i), key)) |val| {
                    try results.append(val);
                }
            }
        }

        pub fn put(self: *Self, key: K, val: V) anyerror!bool {
            const hsh = self.checksum(key);
            var idx = self.prefix(hsh);

            var hold = self.latch.exclusive();
            defer hold.release();

            const d = self.directory;
            var bh = try self.bm.pinLatched(d.bucketPageIDs[idx], .hashBucket, .exclusive);
            defer bh.deinit();

            var b = Bucket.init(bh.page);
            var localIdx: u16 = self.localIndex(hsh);
            if (b.insert(localIdx, key, val)) {
                return true;
            }

            var mirror = try self.bm.allocLatched(.hashBucket, .exclusive);
            defer mirror.deinit();
            var mb = Bucket.new(mirror.page);

            // Replace self.
            var replacement = try self.bm.allocLatched(.hashBucket, .exclusive);
            defer replacement.deinit();
            var rb = Bucket.new(replacement.page);

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
                d.bucketPageIDs[mirrorIdx] = mirror.page.id();
                d.bucketPageIDs[newIdx] = replacement.page.id();
                d.globalDepth += 1;
                // Recalculate insertion idx
                idx = self.prefix(hsh);
            } else {
                // Each of page, mirror should occupy 2**(global depth - old local dopth - 1) spaces
                const toOccupy = @as(u32, 2) << @intCast(u5, d.globalDepth - d.localDepths[idx] - 2);
                var taken: u32 = 0;
                while (taken < toOccupy) : (taken += 1) {
                    d.localDepths[newIdx + taken] = d.localDepths[idx];
                    d.bucketPageIDs[newIdx + taken] = replacement.page.id();
                }
                taken = 0;
                while (taken < toOccupy) : (taken += 1) {
                    d.localDepths[mirrorIdx + taken] = d.localDepths[idx];
                    d.bucketPageIDs[mirrorIdx + taken] = mirror.page.id();
                }
            }

            var i: u16 = 0;
            while (i < Bucket.maxEntries) : (i += 1) {
                if (b.readable[i] == 1) {
                    const e = b.data[i];
                    const ehsh = self.checksum(e.key);
                    if (self.prefix(ehsh) == mirrorIdx) {
                        assert(mb.insert(self.localIndex(ehsh), e.key, e.val));
                    } else {
                        assert(rb.insert(self.localIndex(ehsh), e.key, e.val));
                    }
                }
            }

            // We're splitting so are replacing this page
            try self.bm.free(bh.page.id());

            if (idx == mirrorIdx) {
                return mb.insert(self.localIndex(hsh), key, val);
            } else {
                return try self.put(key, val);
            }
        }

        /// FIXME: merge pages if their load is below a certain amount
        pub fn remove(self: *Self, key: K, val: V) anyerror!void {
            const hsh = self.checksum(key);
            const pfx = self.prefix(hsh);
            var hold = self.latch.shared();
            defer hold.release();

            var base = try self.bm.pinLatched(self.directory.bucketPageIDs[pfx], .hashBucket, .exclusive);
            defer base.deinit();

            var b = Bucket.init(base.page);
            var localIdx: u16 = self.localIndex(hsh);
            if (b.occupied[localIdx] == 0) {
                return;
            }
            b.remove(localIdx, key, val);

            var i = localIdx + 1;
            while (i != localIdx and b.occupied[i] == 1) : (i += 1) {
                if (i > Bucket.maxEntries) {
                    i = 0;
                }
                b.remove(i, key, val);
            }
        }
    };
}
