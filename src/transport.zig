const std = @import("std");
const net = std.net;
const builtin = @import("builtin");
const http2 = struct {
    pub const connection = @import("http2/connection.zig");
    pub const frame = @import("http2/frame.zig");
    pub const stream = @import("http2/stream.zig");
};

pub const TransportError = error{
    ConnectionClosed,
    InvalidHeader,
    PayloadTooLarge,
    CompressionNotSupported,
    Http2Error,
};

pub const Transport = struct {
    stream: net.Stream,
    read_buf: []u8,
    write_buf: []u8,
    allocator: std.mem.Allocator,
    http2_conn: ?http2.connection.Connection,

    /// Read from socket — uses recv() on Windows to avoid ReadFile ERROR_INVALID_PARAMETER
    fn socketRead(self: *Transport, buf: []u8) !usize {
        if (comptime builtin.os.tag == .windows) {
            const rc = std.os.windows.ws2_32.recv(
                @ptrCast(self.stream.handle),
                buf.ptr,
                @intCast(buf.len),
                0,
            );
            if (rc == std.os.windows.ws2_32.SOCKET_ERROR) return error.ConnectionClosed;
            if (rc == 0) return error.ConnectionClosed;
            return @intCast(rc);
        }
        return self.stream.read(buf);
    }

    /// Write to socket — uses send() on Windows to avoid WriteFile issues
    fn socketWrite(self: *Transport, buf: []const u8) !usize {
        if (comptime builtin.os.tag == .windows) {
            const rc = std.os.windows.ws2_32.send(
                @ptrCast(self.stream.handle),
                buf.ptr,
                @intCast(buf.len),
                0,
            );
            if (rc == std.os.windows.ws2_32.SOCKET_ERROR) return error.ConnectionClosed;
            return @intCast(rc);
        }
        return self.stream.write(buf);
    }

    fn socketWriteAll(self: *Transport, buf: []const u8) !void {
        var sent: usize = 0;
        while (sent < buf.len) {
            sent += try self.socketWrite(buf[sent..]);
        }
    }

    pub fn initClient(allocator: std.mem.Allocator, stream: net.Stream) !Transport {
        var transport = Transport{
            .stream = stream,
            .read_buf = try allocator.alloc(u8, 1024 * 64),
            .write_buf = try allocator.alloc(u8, 1024 * 64),
            .allocator = allocator,
            .http2_conn = null,
        };

        // Initialize HTTP/2 connection
        transport.http2_conn = try http2.connection.Connection.init(allocator);
        try transport.setupHttp2Client();

        return transport;
    }

    pub fn initServer(allocator: std.mem.Allocator, stream: net.Stream) !Transport {
        var transport = Transport{
            .stream = stream,
            .read_buf = try allocator.alloc(u8, 1024 * 64),
            .write_buf = try allocator.alloc(u8, 1024 * 64),
            .allocator = allocator,
            .http2_conn = null,
        };

        // Initialize HTTP/2 connection
        transport.http2_conn = try http2.connection.Connection.init(allocator);
        try transport.setupHttp2Server();

        return transport;
    }

    pub fn deinit(self: *Transport) void {
        if (self.http2_conn) |*conn| {
            conn.deinit();
        }
        self.allocator.free(self.read_buf);
        self.allocator.free(self.write_buf);
        self.stream.close();
    }

    fn setupHttp2Client(self: *Transport) !void {
        // Client sends HTTP/2 connection preface
        try self.socketWriteAll(http2.connection.Connection.PREFACE);

        // Send initial SETTINGS frame
        const settings_header: [9]u8 = .{
            0, 0, 0, // length: 0 (no settings parameters)
            @intFromEnum(http2.frame.FrameType.SETTINGS),
            0, // flags: none
            0, 0, 0, 0, // stream_id: 0
        };
        try self.socketWriteAll(&settings_header);
    }

    fn setupHttp2Server(self: *Transport) !void {
        // Read client preface using socketRead (recv on Windows)
        var total: usize = 0;
        while (total < 24) {
            const n = self.socketRead(self.read_buf[total..4096]) catch return TransportError.ConnectionClosed;
            total += n;
        }

        // Validate HTTP/2 connection preface (first 24 bytes)
        if (!std.mem.eql(u8, self.read_buf[0..24], http2.connection.Connection.PREFACE)) {
            return TransportError.Http2Error;
        }

        // Send server SETTINGS + SETTINGS ACK immediately
        const response = [_]u8{
            0, 0, 0, @intFromEnum(http2.frame.FrameType.SETTINGS), 0, 0, 0, 0, 0,
            0, 0, 0, @intFromEnum(http2.frame.FrameType.SETTINGS), 0x1, 0, 0, 0, 0,
        };
        try self.socketWriteAll(&response);
    }

    pub fn readMessage(self: *Transport) ![]const u8 {
        while (true) {
            // Read frame header (9 bytes) into heap-allocated read_buf
            // (avoids Windows ReadFile ERROR_INVALID_PARAMETER with stack buffers)
            var header_read: usize = 0;
            while (header_read < 9) {
                const n = self.socketRead(self.read_buf[header_read..9]) catch return TransportError.ConnectionClosed;
                if (n == 0) return TransportError.ConnectionClosed;
                header_read += n;
            }

            const length: u24 = (@as(u24, self.read_buf[0]) << 16) | (@as(u24, self.read_buf[1]) << 8) | @as(u24, self.read_buf[2]);
            const frame_type = self.read_buf[3];
            const flags = self.read_buf[4];

            // Read payload into heap-allocated buffer
            const payload = try self.allocator.alloc(u8, length);
            if (length > 0) {
                var total_read: usize = 0;
                while (total_read < length) {
                    const remaining = length - total_read;
                    const chunk = @min(remaining, self.read_buf.len);
                    const n = self.socketRead(self.read_buf[0..chunk]) catch {
                        self.allocator.free(payload);
                        return TransportError.ConnectionClosed;
                    };
                    if (n == 0) {
                        self.allocator.free(payload);
                        return TransportError.ConnectionClosed;
                    }
                    @memcpy(payload[total_read .. total_read + n], self.read_buf[0..n]);
                    total_read += n;
                }
            }

            switch (frame_type) {
                @intFromEnum(http2.frame.FrameType.DATA) => {
                    // DATA frame — return payload to handler
                    return payload;
                },
                @intFromEnum(http2.frame.FrameType.HEADERS) => {
                    // HEADERS frame — skip (gRPC routing not yet implemented)
                    self.allocator.free(payload);
                    // Continue to read the DATA frame that follows
                    continue;
                },
                @intFromEnum(http2.frame.FrameType.SETTINGS) => {
                    if (flags & 0x1 == 0) {
                        // SETTINGS frame (not ACK) — send ACK back
                        const settings_ack: [9]u8 = .{
                            0, 0, 0,
                            @intFromEnum(http2.frame.FrameType.SETTINGS),
                            0x1, // ACK flag
                            0, 0, 0, 0,
                        };
                        _ = self.stream.write(&settings_ack) catch {};
                    }
                    self.allocator.free(payload);
                    continue;
                },
                @intFromEnum(http2.frame.FrameType.WINDOW_UPDATE) => {
                    // WINDOW_UPDATE — acknowledge and continue
                    self.allocator.free(payload);
                    continue;
                },
                @intFromEnum(http2.frame.FrameType.PING) => {
                    // PING — send PONG (echo with ACK flag)
                    var pong: [9 + 8]u8 = undefined;
                    pong[0] = 0;
                    pong[1] = 0;
                    pong[2] = 8; // length = 8
                    pong[3] = @intFromEnum(http2.frame.FrameType.PING);
                    pong[4] = 0x1; // ACK flag
                    pong[5] = 0;
                    pong[6] = 0;
                    pong[7] = 0;
                    pong[8] = 0;
                    if (length == 8) @memcpy(pong[9..17], payload[0..8]);
                    self.socketWriteAll(&pong) catch {};
                    self.allocator.free(payload);
                    continue;
                },
                @intFromEnum(http2.frame.FrameType.RST_STREAM) => {
                    self.allocator.free(payload);
                    continue;
                },
                @intFromEnum(http2.frame.FrameType.GOAWAY) => {
                    self.allocator.free(payload);
                    return TransportError.ConnectionClosed;
                },
                @intFromEnum(http2.frame.FrameType.PRIORITY),
                @intFromEnum(http2.frame.FrameType.PUSH_PROMISE),
                @intFromEnum(http2.frame.FrameType.CONTINUATION),
                => {
                    // Skip unsupported frames
                    self.allocator.free(payload);
                    continue;
                },
                else => {
                    // Unknown frame type — skip
                    self.allocator.free(payload);
                    continue;
                },
            }
        }
    }

    pub fn writeMessage(self: *Transport, message: []const u8) !void {
        // HPACK-encoded response headers for gRPC:
        // :status: 200 → static table index 8 (0x88)
        // content-type: application/grpc → literal with name index 31, value "application/grpc"
        const grpc_content_type = "application/grpc";
        const hpack_headers = [_]u8{
            0x88, // :status: 200 (indexed, static table entry 8)
            0x40 | 31, // literal header with incremental indexing, name index 31 (content-type)
            @intCast(grpc_content_type.len), // value length
        } ++ grpc_content_type.*;
        const hpack_len = hpack_headers.len;

        // HEADERS frame with gRPC response headers
        var headers_frame: [9 + hpack_len]u8 = undefined;
        headers_frame[0] = @intCast((hpack_len >> 16) & 0xFF);
        headers_frame[1] = @intCast((hpack_len >> 8) & 0xFF);
        headers_frame[2] = @intCast(hpack_len & 0xFF);
        headers_frame[3] = @intFromEnum(http2.frame.FrameType.HEADERS);
        headers_frame[4] = http2.frame.FrameFlags.END_HEADERS;
        headers_frame[5] = 0;
        headers_frame[6] = 0;
        headers_frame[7] = 0;
        headers_frame[8] = 1; // stream_id: 1
        @memcpy(headers_frame[9..], &hpack_headers);
        try self.socketWriteAll(&headers_frame);

        // DATA frame with gRPC response body
        const data_len: u24 = @intCast(message.len);
        var data_header: [9]u8 = undefined;
        data_header[0] = @intCast((data_len >> 16) & 0xFF);
        data_header[1] = @intCast((data_len >> 8) & 0xFF);
        data_header[2] = @intCast(data_len & 0xFF);
        data_header[3] = @intFromEnum(http2.frame.FrameType.DATA);
        data_header[4] = 0; // no flags yet
        data_header[5] = 0;
        data_header[6] = 0;
        data_header[7] = 0;
        data_header[8] = 1; // stream_id: 1
        try self.socketWriteAll(&data_header);
        if (message.len > 0) {
            try self.socketWriteAll(message);
        }

        // Trailing HEADERS frame with grpc-status: 0 (OK)
        // grpc-status is not in HPACK static table, use literal
        const grpc_status_header = "grpc-status";
        const grpc_status_value = "0";
        const trailer = [_]u8{
            0x00, // literal header, not indexed, new name
            @intCast(grpc_status_header.len),
        } ++ grpc_status_header.* ++ [_]u8{
            @intCast(grpc_status_value.len),
        } ++ grpc_status_value.*;
        const trailer_len = trailer.len;

        var trailer_frame: [9 + trailer_len]u8 = undefined;
        trailer_frame[0] = @intCast((trailer_len >> 16) & 0xFF);
        trailer_frame[1] = @intCast((trailer_len >> 8) & 0xFF);
        trailer_frame[2] = @intCast(trailer_len & 0xFF);
        trailer_frame[3] = @intFromEnum(http2.frame.FrameType.HEADERS);
        trailer_frame[4] = http2.frame.FrameFlags.END_STREAM | http2.frame.FrameFlags.END_HEADERS;
        trailer_frame[5] = 0;
        trailer_frame[6] = 0;
        trailer_frame[7] = 0;
        trailer_frame[8] = 1; // stream_id: 1
        @memcpy(trailer_frame[9..], &trailer);
        try self.socketWriteAll(&trailer_frame);
    }
};


