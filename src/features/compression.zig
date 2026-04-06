const std = @import("std");

pub const CompressionError = error{
    CompressionFailed,
    DecompressionFailed,
    OutOfMemory,
};

pub const Compression = struct {
    pub const Algorithm = enum {
        none,
        gzip,
        deflate,
    };

    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Compression {
        return .{ .allocator = allocator };
    }

    pub fn compress(self: *Compression, data: []const u8, algorithm: Algorithm) ![]u8 {
        return switch (algorithm) {
            .none => self.allocator.dupe(u8, data),
            .gzip, .deflate => self.allocator.dupe(u8, data),
        };
    }

    pub fn decompress(self: *Compression, data: []const u8, algorithm: Algorithm) ![]u8 {
        return switch (algorithm) {
            .none => self.allocator.dupe(u8, data),
            .gzip, .deflate => self.allocator.dupe(u8, data),
        };
    }
};
