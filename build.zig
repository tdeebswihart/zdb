const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const mode = b.standardReleaseOptions();

    const exe = b.addExecutable("zdb", "src/main.zig");
    exe.addPackagePath("libdb", "lib/lib.zig");
    exe.setBuildMode(mode);
    exe.install();

    exe.setOutputDir("zig-cache");

    var main_tests = b.addTest("src/main.zig");
    main_tests.setBuildMode(mode);
    var storage_tests = b.addTest("src/storage/manager.zig");
    storage_tests.addPackagePath("libdb", "lib/lib.zig");
    storage_tests.setBuildMode(mode);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&storage_tests.step);
    test_step.dependOn(&main_tests.step);
}
