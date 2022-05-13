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

pub const DirectoryPage = packed struct {
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
    const Entry = packed struct {
        key: K,
        val: V,
    };
    return packed struct {
        pub const maxEntries = 4 * PAGE_SIZE / (4 * @sizeOf(Entry) + 1) / 8;
        header: page.Header,

        occupied: [maxEntries]u8,
        // 0 if tombstoned or unoccupied
        // 1 otherwise
        readable: [maxEntries]u8,
        data: [maxEntries * 8]Entry,
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

        pub fn unoccupied(self: *Self, i: u16) bool {
            return self.occupied[i / 8] & (@as(u8, 1) << @truncate(u3, i)) == 0;
        }

        pub fn unreadable(self: *Self, i: u16) bool {
            return self.readable[i / 8] & (@as(u8, 1) << @truncate(u3, i)) == 0;
        }

        pub fn get(self: *Self, i: u16, key: K) ?V {
            if (self.unreadable(i)) {
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
            while (i != initIdx) {
                if (self.put(i, key, val)) {
                    return true;
                }
                i += 1;
                if (i > Self.maxEntries) {
                    i = 0;
                }
            }
            return false;
        }

        fn put(self: *Self, i: u16, key: K, val: V) bool {
            const byte: u16 = i / 8;
            const bit: u3 = @truncate(u3, i);
            if (self.unoccupied(i)) {
                self.occupied[byte] |= @as(u8, 1) << bit;
                self.readable[byte] |= @as(u8, 1) << bit;
                self.data[i] = .{ .key = key, .val = val };
                return true;
            }
            return false;
        }

        pub fn remove(self: *Self, i: u16, key: K, val: V) void {
            var e = &self.data[i];
            const bit: u3 = @truncate(u3, i);
            if (!self.unreadable(i) and meta.eql(key, e.key) and meta.eql(val, e.val)) {
                self.readable[i / 8] &= ~(@as(u8, 1) << bit);
            }
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

            var b2 = try bm.allocLatched(.hashBucket, .exclusive);
            defer b2.deinit();
            _ = Bucket.new(b2.page);

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
                const pages = @as(u32, 2) << @truncate(u5, self.directory.globalDepth - 1);
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
            while (i != localIdx and !b.unoccupied(i)) : (i += 1) {
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

            const d = self.directory;
            log.debug("getting idx={d} page={d}", .{ idx, d.bucketPageIDs[idx] });
            var bh = try self.bm.pinLatched(d.bucketPageIDs[idx], .hashBucket, .exclusive);

            var b = Bucket.init(bh.page);
            var localIdx: u16 = self.localIndex(hsh);
            if (b.insert(localIdx, key, val)) {
                bh.deinit();
                hold.release();
                return true;
            }

            var mirror = try self.bm.allocLatched(.hashBucket, .exclusive);
            var mb = Bucket.new(mirror.page);

            // Replace self.
            var replacement = try self.bm.allocLatched(.hashBucket, .exclusive);
            var rb = Bucket.new(replacement.page);
            log.debug("splitting {d} idx {d} into {d} and {d}", .{ d.bucketPageIDs[idx], idx, mirror.page.id(), replacement.page.id() });

            d.localDepths[idx] += 1;
            var newIdx = idx;
            if (d.localDepths[idx] >= d.globalDepth) {
                newIdx <<= 1;
                // Double in size
                // Starting from the last active page, each gets
                // (idx << 1) and (idx << 1 + 1)
                // We then overwrite mirrorIdx with mirror's pageId
                var last = (@as(u32, 2) << @truncate(u5, d.globalDepth - 1)) - 1;
                log.debug("doubling size from global depth {d}; last at {d}", .{ d.globalDepth, last });
                while (last > 0) : (last -= 1) {
                    log.debug("moving bucket page {d} from {d} to idx {d}", .{ d.bucketPageIDs[last], last, last << 1 });
                    d.bucketPageIDs[last << 1] = d.bucketPageIDs[last];
                    d.localDepths[last << 1] = d.localDepths[last];
                    log.debug("moving bucket page {d} from {d} to idx {d}", .{ d.bucketPageIDs[last], last, (last << 1) + 1 });
                    d.bucketPageIDs[(last << 1) + 1] = d.bucketPageIDs[last];
                    d.localDepths[(last << 1) + 1] = d.localDepths[last];
                }
                // handle bucket zero now
                log.debug("moving bucket page {d} from 0 to idx 1", .{d.bucketPageIDs[last]});
                d.bucketPageIDs[1] = d.bucketPageIDs[last];
                d.localDepths[1] = d.localDepths[last];

                d.globalDepth += 1;
            }
            // Each of page, mirror should occupy 2**(global depth - old local depth - 1) spaces
            const toOccupy = @as(u32, 2) << @truncate(u5, d.globalDepth - d.localDepths[idx]);
            const mirrorIdx = newIdx + toOccupy;
            var taken: u32 = 0;
            while (taken < toOccupy) : (taken += 1) {
                d.localDepths[newIdx + taken] = d.localDepths[idx];
                log.debug("placing replacement page {d} depth {d} at idx {d}", .{ replacement.page.id(), d.localDepths[idx], newIdx + taken });
                d.bucketPageIDs[newIdx + taken] = replacement.page.id();
            }
            taken = 0;
            while (taken < toOccupy) : (taken += 1) {
                d.localDepths[mirrorIdx + taken] = d.localDepths[idx];
                log.debug("placing mirror page {d} depth {d} at idx {d}", .{ mirror.page.id(), d.localDepths[idx], mirrorIdx + taken });
                d.bucketPageIDs[mirrorIdx + taken] = mirror.page.id();
            }
            // Recalculate insertion idx
            idx = self.prefix(hsh);

            var i: u16 = 0;
            while (i < Bucket.maxEntries) : (i += 1) {
                if (!b.unreadable(i)) {
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
            const origPage = bh.page.id();
            bh.deinit();
            try self.bm.free(origPage);

            replacement.deinit();
            mirror.deinit();
            hold.release();
            return try self.put(key, val);
        }

        /// FIXME: merge pages if their load is below a certain amount
        pub fn remove(self: *Self, key: K, val: V) anyerror!void {
            const hsh = self.checksum(key);
            const pfx = self.prefix(hsh);
            var hold = self.latch.shared();
            defer hold.release();

            log.debug("latching shared bucketPage={d}", .{self.directory.bucketPageIDs[pfx]});
            var base = try self.bm.pinLatched(self.directory.bucketPageIDs[pfx], .hashBucket, .exclusive);
            defer base.deinit();

            var b = Bucket.init(base.page);
            var localIdx: u16 = self.localIndex(hsh);
            if (b.unoccupied(localIdx)) {
                return;
            }
            b.remove(localIdx, key, val);

            var i = localIdx + 1;
            // FIXME
            while (i != localIdx and !b.unoccupied(i)) : (i += 1) {
                if (i > Bucket.maxEntries) {
                    i = 0;
                }
                b.remove(i, key, val);
            }
        }
    };
}
