const std = @import("std");
const Entry = @import("entry.zig").Entry;

pub const Record = struct {
    // The offset of this Record within its block
    offset: u16,
    // Entries can be at most 1 block in size
    // A size of -1 means "deleted"
    size: u16,
};

pub const BlockError = error {
    OutOfSpace,
};

pub const Header = struct {
    remaining_space: u16 = 0,
    free_space: u16 = 0,
    active_records: u32 = 0,

    pub fn invalid(self: Header) bool {
        return (self.active_records == 0) and (self.free_space == 0);
    }
};

pub const GetError = error {
    RecordDoesntExist,
    RecordDeleted,
};

// Each block is of fixed length and acts as a bump-up
// allocator for contained records.
// A periodic compaction (or vacuum) process should clean up
// deleted blocks so there are no gaps in the file.
pub fn Block(comptime T: type) type {
    return struct {
        file: T,
        offset: u64,
        size: u16,
        header: Header,
        records: std.ArrayList(Record),
        mem: *std.mem.Allocator,

        const Self = @This();

        pub fn init(file: T, offset: u64, size: u16, mem: *std.mem.Allocator) !*Self {
            try file.seekTo(offset);

            var hdr = Header{};
            _ = try file.read(std.mem.asBytes(&hdr));
            const records = blk: {
                if (hdr.invalid()) {
                    hdr.free_space = size;
                    hdr.remaining_space = size - @sizeOf(Header);
                    break :blk std.ArrayList(Record).init(mem);
                }
                var recs = std.ArrayList(Record).init(mem);
                try recs.resize(hdr.active_records);
                _ = try file.readAll(std.mem.sliceAsBytes(recs.items));
                break :blk recs;
            };

            std.debug.assert(hdr.active_records == records.items.len);

            var b = try mem.create(Self);
            b.file = file;
            b.offset = offset;
            b.size = size;
            b.header = hdr;
            b.records = records;
            b.mem = mem;
            return b;
        }

        pub fn deinit(self: *Self) !void {
            try self.file.seekTo(self.offset);
            _ = try self.file.writeAll(std.mem.asBytes(&self.header));
            _ = try self.file.writeAll(std.mem.sliceAsBytes(self.records.items));
            self.records.deinit();
            self.mem.destroy(self);
        }

        pub fn can_contain(self: *Self, amount: u16) bool {
            return self.header.remaining_space > (amount + @sizeOf(Record));
        }

        pub fn put(self: *Self, record: []const u8) !Entry {
            const sz = @intCast(u16, record.len) + @sizeOf(Record);
            if (self.header.remaining_space < sz) {
                return BlockError.OutOfSpace;
            }

            self.header.free_space -= @intCast(u16, record.len);
            const offset = self.offset + self.header.free_space;

            try self.file.seekTo(offset);
            try self.file.writeAll(record);

            const record_num = self.records.items.len;

            try self.records.append(Record{
                .offset = self.header.free_space,
                .size = @intCast(u16, record.len),
            });

            self.header.active_records += 1;
            std.debug.assert(self.header.active_records == self.records.items.len);

            self.header.remaining_space -= sz;

            return Entry{
                .record = @intCast(u16, record_num),
            };
        }

        pub fn get(self: *Self, record: u16) ![]const u8 {
            if (record > self.header.active_records) {
                return GetError.RecordDoesntExist;
            }
            const rec = self.records.items[record];
            if (rec.size == -1) {
                return GetError.RecordDeleted;
            }
            try self.file.seekTo(self.offset + rec.offset);
            const buf = try self.mem.alloc(u8, rec.size);
            _ = try self.file.readAll(buf);
            return buf;
        }
    };
}
