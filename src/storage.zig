pub const Manager = @import("storage/manager.zig").Manager;
const FSFile = @import("storage/file.zig").FSFile;
pub const FileManager = Manager(FSFile);
