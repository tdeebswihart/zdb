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

    var lib_tests = b.addTest("lib/sync/latch.zig");
    lib_tests.setBuildMode(mode);

    var storage_tests = b.addTest("src/storage/tests.zig");
    storage_tests.addPackagePath("libdb", "lib/lib.zig");
    storage_tests.setBuildMode(mode);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&storage_tests.step);
    test_step.dependOn(&lib_tests.step);

    const run_cmd = exe.run();
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the main entrypoint");
    run_step.dependOn(&run_cmd.step);
}
