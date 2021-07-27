const std = @import("std");
const fs = std.fs;

pub fn File(comptime Context: type,
            comptime readFn: anytype,comptime ReadError: anytype,
            comptime writeFn: anytype, comptime WriteError: anytype,
            comptime seekFn: anytype,comptime SeekError: anytype,
            comptime extendFn: anytype, comptime ExtendError: anytype,
            comptime sizeFn: anytype,comptime SizeError: anytype,
) type {
    return struct {
        context: Context,

        const Self = @This();
        pub fn read(self: Self, buffer: []u8) ReadError!usize {
            return readFn(self.context, buffer);
        }

        pub fn readAll(self: Self, buffer: []u8) ReadError!usize {
            var index: usize = 0;
            while (index != buffer.len) {
                const amt = try self.read(buffer[index..]);
                if (amt == 0) return index;
                index += amt;
            }
            return index;
        }

        pub fn write(self: Self, buffer: []const u8) WriteError!usize {
            return writeFn(self.context, buffer);
        }

        pub fn writeAll(self: Self, buffer: []const u8) WriteError!void {
            var index: usize = 0;
            while (index != buffer.len) {
                const amt = try self.write(buffer[index..]);
                if (amt == 0) break;
                index += amt;
            }
        }

        pub fn seekTo(self: Self, pos: usize) SeekError!void {
            return seekFn(self.context, pos);
        }

        pub fn extend(self: Self, sz: usize) ExtendError!void {
            return extendFn(self.context, sz);
        }

        pub fn size(self: Self) SizeError!u64 {
            return sizeFn(self.context);
        }
    };
}

pub fn getFileSize(f: fs.File) fs.File.StatError!u64 {
    const st = try f.stat();
    return st.size;
}

pub const FSFile = File(
    fs.File,
    fs.File.read, fs.File.ReadError,
    fs.File.write, fs.File.WriteError,
    fs.File.seekTo, fs.File.SeekError,
    fs.File.setEndPos, fs.File.SetEndPosError,
    getFileSize, fs.File.StatError,
);
