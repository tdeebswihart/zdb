pub const ResultTag = enum { ok, err };
pub fn Result(comptime T: type, comptime E: type) type {
    return union(ResultTag) { ok: T, err: E };
}
