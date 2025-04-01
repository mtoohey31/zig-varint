const std = @import("std");
const debug = std.debug;
const io = std.io;
const math = std.math;
const mem = std.mem;
const testing = std.testing;

fn Unsigned(comptime T: type) type {
    switch (@typeInfo(T)) {
        .int => |int| {
            var unsigned_int = int;
            unsigned_int.signedness = .unsigned;
            return @Type(.{ .int = unsigned_int });
        },
        else => @compileError("expected T to have integer type"),
    }
}

/// Decode the varint within buf, without checking for overflow and assuming
/// the varint in buf is properly terminated. buf will be reassigned to the
/// subset of its original value following the bytes that contained the varint.
pub fn decodeUnchecked(comptime T: type, buf: *[]const u8) T {
    const signedness = switch (@typeInfo(T)) {
        .int => |int| int.signedness,
        else => @compileError("expected T to have integer type"),
    };

    var acc: Unsigned(T) = 0;
    var shift_amt: math.Log2Int(Unsigned(T)) = 0;
    var curr_buf = buf.*;
    while (true) {
        debug.assert(curr_buf.len > 0);
        const x = curr_buf[0];
        acc |= @as(Unsigned(T), @intCast(x & 0x7F)) << shift_amt;

        curr_buf = curr_buf[1..];

        if (x < 0x80) {
            break;
        }

        shift_amt += 7;
    }
    buf.* = curr_buf;

    switch (signedness) {
        .signed => {
            var signed: T = @intCast(acc >> 1);
            if ((acc & 1) != 0) {
                signed = ~signed;
            }
            return signed;
        },
        .unsigned => return acc,
    }
}

test decodeUnchecked {
    {
        var buf: []const u8 = &[_]u8{ 204, 49 };
        try testing.expectEqual(3174, decodeUnchecked(i16, &buf));
    }

    {
        var buf: []const u8 = &[_]u8{ 203, 49 };
        try testing.expectEqual(-3174, decodeUnchecked(i16, &buf));
    }

    {
        var buf: []const u8 = &[_]u8{ 215, 26 };
        try testing.expectEqual(3415, decodeUnchecked(u32, &buf));
    }
}

pub const DecodeError = error{ EndOfStream, Overflow };

pub fn read(
    comptime T: type,
    reader: anytype,
) (@TypeOf(reader).NoEofError || DecodeError)!T {
    const signedness = switch (@typeInfo(T)) {
        .int => |int| int.signedness,
        else => @compileError("expected T to have integer type"),
    };

    var acc: Unsigned(T) = 0;
    var shift_amt: ?math.Log2Int(Unsigned(T)) = 0;
    while (true) {
        const x = try reader.readByte();
        if (shift_amt) |s| {
            acc |= try math.shlExact(Unsigned(T), x & 0x7F, s);
        } else if (x & 0x7F != 0) {
            return error.Overflow;
        }

        if (x < 0x80) {
            break;
        }

        if (shift_amt) |s| {
            shift_amt = math.add(math.Log2Int(Unsigned(T)), s, 7) catch null;
        }
    }

    switch (signedness) {
        .signed => {
            var signed: T = @intCast(acc >> 1);
            if ((acc & 1) != 0) {
                signed = ~signed;
            }
            return signed;
        },
        .unsigned => return acc,
    }
}

test read {
    {
        var stream = io.fixedBufferStream(&[_]u8{ 204, 49 });
        try testing.expectEqual(3174, try read(i16, stream.reader()));
    }

    {
        var stream = io.fixedBufferStream(&[_]u8{ 203, 49 });
        try testing.expectEqual(-3174, try read(i16, stream.reader()));
    }

    {
        var stream = io.fixedBufferStream(&[_]u8{ 215, 26 });
        try testing.expectEqual(3415, try read(u32, stream.reader()));
    }
}

/// Decode the varint within buf. buf will be reassigned to the subset of its
/// original value following the bytes that contained the varint.
pub fn decode(comptime T: type, buf: *[]const u8) DecodeError!T {
    var stream = io.fixedBufferStream(buf.*);
    const res = try read(T, stream.reader());
    buf.* = buf.*[try stream.getPos()..];
    return res;
}

test decode {
    {
        var buf: []const u8 = &[_]u8{ 204, 49 };
        try testing.expectEqual(3174, try decode(i16, &buf));
    }

    {
        var buf: []const u8 = &[_]u8{ 203, 49 };
        try testing.expectEqual(-3174, try decode(i16, &buf));
    }

    {
        var buf: []const u8 = &[_]u8{ 215, 26 };
        try testing.expectEqual(3415, try decode(u32, &buf));
    }
}

/// Returns the length of x in bytes when encoded as a varint.
pub fn encodedLen(comptime T: type, x: T) usize {
    const sign_bits: comptime_int = comptime switch (@typeInfo(T)) {
        .int => |int| switch (int.signedness) {
            .signed => 1,
            .unsigned => 0,
        },
        else => @compileError("expected T to have integer type"),
    };
    if (x == 0) return 1;
    const bits = sign_bits + math.log2_int_ceil(Unsigned(T), @abs(x));
    return math.divCeil(usize, bits, 7) catch {
        unreachable;
    };
}

test encodedLen {
    try testing.expectEqual(1, encodedLen(u32, 0));
    try testing.expectEqual(2, encodedLen(u32, 256));
    try testing.expectEqual(2, encodedLen(u32, 153));
    try testing.expectEqual(1, encodedLen(u32, 128));
    try testing.expectEqual(2, encodedLen(i32, 128));
    try testing.expectEqual(2, encodedLen(i32, 128));
}

/// Encode x into buf. Returns the subset of buf following the bytes written
/// for x.
///
/// Asserts: encodedLen(T, n) <= buf.len
pub fn encodeUnchecked(comptime T: type, x: T, buf: []u8) []u8 {
    debug.assert(encodedLen(T, x) <= buf.len);

    const signedness = switch (@typeInfo(T)) {
        .int => |int| int.signedness,
        else => @compileError("expected T to have integer type"),
    };

    var y: Unsigned(T) = blk: {
        switch (signedness) {
            .signed => {
                var y = @as(Unsigned(T), @bitCast(x)) << 1;
                if (x < 0) {
                    y = ~y;
                }
                break :blk y;
            },
            .unsigned => break :blk x,
        }
    };
    var curr_buf = buf;
    while (y >= 0x80) {
        curr_buf[0] = @as(u8, @truncate(y & 0b111_1111)) | 0b1000_0000;
        curr_buf = curr_buf[1..];
        y >>= 7;
    }
    curr_buf[0] = @truncate(y);
    return curr_buf[1..];
}

pub const BufEncodeError = error{NoSpaceLeft};

/// Encode x into buf, throwing error.NoSpaceLeft if it won't fit. Returns the
/// subset of buf following the bytes written for x.
pub fn bufEncode(comptime T: type, x: T, buf: []u8) BufEncodeError![]u8 {
    if (encodedLen(T, x) > buf.len) {
        return error.NoSpaceLeft;
    }

    return encodeUnchecked(T, x, buf);
}

/// Encode x into a fresh buffer of the exact size required, allocated with
/// alloc.
pub fn allocEncode(comptime T: type, alloc: mem.Allocator, x: T) mem.Allocator.Error![]u8 {
    const buf = try alloc.alloc(u8, encodedLen(T, x));
    _ = encodeUnchecked(T, x, buf);
    return buf;
}

test allocEncode {
    {
        var buf: []const u8 = try allocEncode(u32, testing.allocator, 153);
        const original_buf = buf;
        defer testing.allocator.free(original_buf);
        try testing.expectEqualDeep(153, try decode(u32, &buf));
        try testing.expectEqual(original_buf[2..], buf);
    }

    {
        const buf = try allocEncode(u32, testing.allocator, 3415);
        defer testing.allocator.free(buf);
        try testing.expectEqualDeep(&[_]u8{ 215, 26 }, buf);
    }

    {
        const buf = try allocEncode(i16, testing.allocator, -3415);
        defer testing.allocator.free(buf);
        try testing.expectEqualDeep(&[_]u8{ 173, 53 }, buf);
    }
}

test "fuzz decodeUnchecked if decode u32" {
    try std.testing.fuzz(@as(u1, 0), struct {
        fn f(_: u1, input: []const u8) anyerror!void {
            var buf = input;
            const expected = decode(u32, &buf) catch {
                return;
            };
            buf = input;
            try testing.expectEqual(expected, decodeUnchecked(u32, &buf));
        }
    }.f, .{});
}

test "fuzz decodeUnchecked if decode i64" {
    try std.testing.fuzz(@as(u1, 0), struct {
        fn f(_: u1, input: []const u8) anyerror!void {
            var buf = input;
            const expected = decode(i64, &buf) catch {
                return;
            };
            buf = input;
            try testing.expectEqual(expected, decodeUnchecked(i64, &buf));
        }
    }.f, .{});
}

test "fuzz decode inverse encode u64" {
    try std.testing.fuzz(@as(u1, 0), struct {
        fn f(_: u1, input: []const u8) anyerror!void {
            const expected = std.hash.Murmur2_64.hash(input);
            var buf: []const u8 = allocEncode(u64, testing.allocator, expected) catch {
                return;
            };
            const original_buf = buf;
            defer testing.allocator.free(original_buf);
            try testing.expectEqual(expected, decodeUnchecked(u64, &buf));
        }
    }.f, .{});
}

test "fuzz decode inverse encode i32" {
    try std.testing.fuzz(@as(u1, 0), struct {
        fn f(_: u1, input: []const u8) anyerror!void {
            const expected: i32 = @bitCast(std.hash.Murmur2_32.hash(input));
            var buf: []const u8 = allocEncode(i32, testing.allocator, expected) catch {
                return;
            };
            const original_buf = buf;
            defer testing.allocator.free(original_buf);
            try testing.expectEqual(expected, decodeUnchecked(i32, &buf));
        }
    }.f, .{});
}

/// Encode x into a fresh buffer with a null terminator of the exact size
/// required, allocated with alloc.
pub fn allocEncodeZ(comptime T: type, alloc: mem.Allocator, x: T) mem.Allocator.Error![:0]u8 {
    const buf = try alloc.allocSentinel(u8, encodedLen(T, x), 0);
    _ = encodeUnchecked(T, x, buf);
    return buf;
}

/// Write x to writer.
pub fn write(comptime T: type, x: T, writer: anytype) @TypeOf(writer).Error!void {
    const signedness = switch (@typeInfo(T)) {
        .int => |int| int.signedness,
        else => @compileError("expected T to have integer type"),
    };

    var y: Unsigned(T) = blk: {
        switch (signedness) {
            .signed => {
                var y = @as(Unsigned(T), @bitCast(x)) << 1;
                if (x < 0) {
                    y = ~y;
                }
                break :blk y;
            },
            .unsigned => break :blk x,
        }
    };
    while (y >= 0x80) {
        try writer.writeByte(@as(u8, @truncate(y & 0b111_1111)) | 0b1000_0000);
        y >>= 7;
    }
    try writer.writeByte(@truncate(y));
}

test write {
    var array_list = std.ArrayList(u8).init(testing.allocator);
    defer array_list.deinit();

    {
        try write(u32, 153, array_list.writer());
        var buf: []const u8 = array_list.items;
        try testing.expectEqualDeep(153, try decode(u32, &buf));
        try testing.expectEqual(array_list.items[2..], buf);
        array_list.clearAndFree();
    }

    {
        try write(u32, 3415, array_list.writer());
        try testing.expectEqualDeep(&[_]u8{ 215, 26 }, array_list.items);
        array_list.clearAndFree();
    }

    {
        try write(i16, -3415, array_list.writer());
        try testing.expectEqualDeep(&[_]u8{ 173, 53 }, array_list.items);
        array_list.clearAndFree();
    }
}
