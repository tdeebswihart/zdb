const builtin = @import("builtin");
const std = @import("std");
const fs = std.fs;
const os = std.os;
const fcntl = @cImport({
    @cInclude("fcntl.h");
});

pub const ReadError = error{
    AccessDenied,
    Interrupted,
    NotOpenForReading,
    Unexpected,
};

pub const WriteError = error{
    NoSpaceLeft,
    AccessDenied,
    Unexpected,
    NotOpenForWriting,
};

pub const SeekError = error{
    Unseekable,
    Unexpected,
};

pub const ExtendError = error{
    TooBig,
    Busy,
    Unexpected,
    AccessDenied,
};

pub const SizeError = error{
    AccessDenied,
    Unexpected,
};

pub const Store = struct {
    readFn: fn (*Store, []u8) ReadError!usize,
    writeFn: fn (*Store, []const u8) WriteError!usize,
    seekFn: fn (*Store, usize) SeekError!void,
    extendFn: fn (*Store, usize) ExtendError!void,
    sizeFn: fn (*Store) SizeError!usize,

    const Self = @This();
    pub fn read(self: *Self, buffer: []u8) ReadError!usize {
        return self.readFn(self, buffer);
    }

    pub fn readAll(self: *Self, buffer: []u8) ReadError!usize {
        var index: usize = 0;
        while (index != buffer.len) {
            const amt = try self.readFn(self, buffer[index..]);
            if (amt == 0) return index;
            index += amt;
        }
        return index;
    }

    pub fn write(self: *Self, buffer: []const u8) WriteError!usize {
        return self.writeFn(self, buffer);
    }

    pub fn writeAll(self: *Self, buffer: []const u8) WriteError!void {
        var index: usize = 0;
        while (index != buffer.len) {
            const amt = try self.writeFn(self, buffer[index..]);
            if (amt == 0) break;
            index += amt;
        }
    }

    pub fn seekTo(self: *Self, pos: usize) SeekError!void {
        return self.seekFn(self, pos);
    }

    pub fn extend(self: *Self, sz: usize) ExtendError!void {
        return self.extendFn(self, sz);
    }

    pub fn size(self: *Self) SizeError!usize {
        return self.sizeFn(self);
    }
};

fn initDirectIO(f: fs.File) !void {
    switch (builtin.os.tag) {
        .macos => {
            _ = try os.fcntl(f.handle, fcntl.F_NOCACHE, 0);
        },
        .linux => {
            _ = try os.fcntl(f.handle, fcntl.O_DIRECT, 0);
        },
        else => @compileError("os not supported"),
    }
}

pub const File = struct {
    f: fs.File,
    store: Store,

    const Self = @This();

    pub fn init(f: fs.File) !Self {
        try initDirectIO(f);
        return Self{
            .f = f,
            .store = .{
                .readFn = readImpl,
                .writeFn = writeImpl,
                .seekFn = seekImpl,
                .extendFn = extendImpl,
                .sizeFn = sizeImpl,
            },
        };
    }

    const FReadError = fs.File.ReadError;
    fn readImpl(store: *Store, buffer: []u8) ReadError!usize {
        const self: fs.File = @fieldParentPtr(Self, "store", store).f;
        return self.read(buffer) catch |err| return switch (err) {
            FReadError.AccessDenied => ReadError.AccessDenied,
            FReadError.NotOpenForReading => ReadError.NotOpenForReading,
            else => ReadError.Unexpected,
        };
    }

    const FWriteError = fs.File.WriteError;
    fn writeImpl(store: *Store, buffer: []const u8) WriteError!usize {
        const self: fs.File = @fieldParentPtr(Self, "store", store).f;
        return self.write(buffer) catch |err| return switch (err) {
            FWriteError.AccessDenied => WriteError.AccessDenied,
            FWriteError.NotOpenForWriting => WriteError.NotOpenForWriting,
            FWriteError.NoSpaceLeft => WriteError.NoSpaceLeft,
            else => WriteError.Unexpected,
        };
    }

    const FSeekError = fs.File.SeekError;
    fn seekImpl(store: *Store, pos: usize) SeekError!void {
        const self: fs.File = @fieldParentPtr(Self, "store", store).f;
        return self.seekTo(pos) catch |err| return switch (err) {
            FSeekError.Unseekable => SeekError.Unseekable,
            else => SeekError.Unexpected,
        };
    }

    const FExtendError = fs.File.SetEndPosError;
    fn extendImpl(store: *Store, sz: usize) ExtendError!void {
        const self: fs.File = @fieldParentPtr(Self, "store", store).f;
        return self.setEndPos(sz) catch |err| return switch (err) {
            FExtendError.AccessDenied => ExtendError.AccessDenied,
            FExtendError.FileTooBig => ExtendError.TooBig,
            FExtendError.FileBusy => ExtendError.Busy,
            else => ExtendError.Unexpected,
        };
    }

    const FSizeError = fs.File.StatError;
    fn sizeImpl(store: *Store) SizeError!usize {
        const self: fs.File = @fieldParentPtr(Self, "store", store).f;
        const st = self.stat() catch |err| return switch (err) {
            FSizeError.AccessDenied => SizeError.AccessDenied,
            else => SizeError.Unexpected,
        };
        return st.size;
    }
};
