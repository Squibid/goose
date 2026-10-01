const std = @import("std");
const goose = @import("goose");
const Connection = goose.Connection;
const message = goose.message;
const GUFd = goose.core.value.GUFd;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    std.debug.print("=== Test Suite: Out-of-Band UNIX File Descriptor Transport (SCM_RIGHTS) ===\n", .{});

    var conn = try Connection.init(allocator, .Session, init.io, init.environ_map);
    defer conn.close();

    if (!conn.supports_unix_fd) {
        std.debug.print("FAIL: Bus does not support UNIX FD passing (NEGOTIATE_UNIX_FD rejected)!\n", .{});
        return error.UnixFdNotSupported;
    }
    std.debug.print("PASS: NEGOTIATE_UNIX_FD handshake succeeded.\n", .{});

    // -------------------------------------------------------------
    // Test 1: Single FD Passing & takeFds() Ownership Transfer
    // -------------------------------------------------------------
    std.debug.print("\n--- Subtest 1: Single FD Passing & takeFds() Transfer ---\n", .{});
    {
        const FdTracker = struct {
            received: std.atomic.Value(bool) = .init(false),
            matched: std.atomic.Value(bool) = .init(false),
            taken_fd: std.atomic.Value(std.posix.fd_t) = .init(-1),

            fn callback(ctx_ptr: ?*anyopaque, msg: goose.core.Message) void {
                const self: *@This() = @ptrCast(@alignCast(ctx_ptr.?));

                if (msg.fds.len == 0) return;
                self.received.store(true, .release);

                var d = message.BodyDecoder.fromMessage(std.heap.page_allocator, msg);
                const ufd = d.decode(GUFd) catch return;

                const os_fd = msg.getFd(ufd) orelse return;
                var buf: [64]u8 = undefined;
                const n = std.posix.read(os_fd, &buf) catch return;
                if (std.mem.eql(u8, buf[0..n], "hello unix fd transport via scm_rights")) {
                    self.matched.store(true, .release);
                }

                var mut_msg = msg;
                const taken = mut_msg.takeFds(std.heap.page_allocator) catch return;
                if (taken.len > 0) {
                    self.taken_fd.store(taken[0], .release);
                    std.heap.page_allocator.free(taken);
                }
            }
        };

        var tracker = FdTracker{};
        try conn.addMatch("type='signal',interface='dev.goose.test.FdPassing',member='FdSignal'");
        try conn.registerSignalHandler("dev.goose.test.FdPassing", "FdSignal", FdTracker.callback, &tracker);

        var pipe_fds: [2]std.posix.fd_t = undefined;
        const pipe_rc = std.posix.system.pipe(&pipe_fds);
        if (std.posix.errno(pipe_rc) != .SUCCESS) return error.PipeFailed;
        defer _ = std.posix.system.close(pipe_fds[1]);

        const sentinel = "hello unix fd transport via scm_rights";
        const write_rc = std.posix.system.write(pipe_fds[1], sentinel.ptr, sentinel.len);
        if (std.posix.errno(write_rc) != .SUCCESS) return error.WriteFailed;

        var encoder = try message.BodyEncoder.encode(allocator, GUFd.new(0));
        defer encoder.deinit();

        const serial = conn.nextSerial();
        const header = goose.core.MessageHeader{
            .message_type = .Signal,
            .flags = 0,
            .proto_version = 1,
            .body_length = @intCast(encoder.body().len),
            .serial = serial,
            .header_fields = @constCast(&[_]goose.core.HeaderField{
                .{ .code = .Path, .value = .{ .Path = "/dev/goose/test/FdPassing" } },
                .{ .code = .Interface, .value = .{ .Interface = "dev.goose.test.FdPassing" } },
                .{ .code = .Member, .value = .{ .Member = "FdSignal" } },
                .{ .code = .Signature, .value = .{ .Signature = encoder.signature() } },
            }),
        };

        const fds_to_send = [_]std.posix.fd_t{pipe_fds[0]};
        const msg = goose.core.Message.newWithFds(header, encoder.body(), &fds_to_send);
        try conn.sendMessage(msg);
        _ = std.posix.system.close(pipe_fds[0]);

        var attempts: usize = 0;
        while (attempts < 100) : (attempts += 1) {
            if (tracker.received.load(.acquire) and tracker.matched.load(.acquire) and tracker.taken_fd.load(.acquire) >= 0) break;
            var req = std.os.linux.timespec{ .sec = 0, .nsec = 10 * std.time.ns_per_ms };
            _ = std.os.linux.nanosleep(&req, null);
        }

        if (!tracker.received.load(.acquire)) {
            std.debug.print("FAIL: Did not receive message with file descriptor!\n", .{});
            return error.FdNotReceived;
        }
        if (!tracker.matched.load(.acquire)) {
            std.debug.print("FAIL: Payload read from received file descriptor did not match sentinel!\n", .{});
            return error.FdPayloadMismatch;
        }
        const t_fd = tracker.taken_fd.load(.acquire);
        if (t_fd >= 0) {
            defer _ = std.posix.system.close(t_fd);
            std.debug.print("PASS: Received FD ({d}) and verified sentinel payload.\n", .{t_fd});
            std.debug.print("PASS: Successfully transferred ownership via takeFds().\n", .{});
        } else {
            return error.TakeFdsFailed;
        }
    }

    // -------------------------------------------------------------
    // Test 2: Multiple FDs in a Single Message
    // -------------------------------------------------------------
    std.debug.print("\n--- Subtest 2: Multiple File Descriptors (2 FDs) ---\n", .{});
    {
        const MultiFdTracker = struct {
            received: std.atomic.Value(bool) = .init(false),
            matched_a: std.atomic.Value(bool) = .init(false),
            matched_b: std.atomic.Value(bool) = .init(false),
            count: std.atomic.Value(usize) = .init(0),

            fn callback(ctx_ptr: ?*anyopaque, msg: goose.core.Message) void {
                const self: *@This() = @ptrCast(@alignCast(ctx_ptr.?));

                if (msg.fds.len != 2) return;
                self.received.store(true, .release);
                self.count.store(msg.fds.len, .release);

                var d = message.BodyDecoder.fromMessage(std.heap.page_allocator, msg);
                const FdPair = struct { a: GUFd, b: GUFd };
                const pair = d.decode(FdPair) catch return;

                const fd_a = msg.getFd(pair.a) orelse return;
                const fd_b = msg.getFd(pair.b) orelse return;

                var buf_a: [32]u8 = undefined;
                const na = std.posix.read(fd_a, &buf_a) catch return;
                if (std.mem.eql(u8, buf_a[0..na], "multi-fd payload A")) {
                    self.matched_a.store(true, .release);
                }

                var buf_b: [32]u8 = undefined;
                const nb = std.posix.read(fd_b, &buf_b) catch return;
                if (std.mem.eql(u8, buf_b[0..nb], "multi-fd payload B")) {
                    self.matched_b.store(true, .release);
                }

                var mut_msg = msg;
                const fds = mut_msg.takeFds(std.heap.page_allocator) catch return;
                defer std.heap.page_allocator.free(fds);
                for (fds) |f| {
                    _ = std.posix.system.close(f);
                }
            }
        };

        var tracker = MultiFdTracker{};
        try conn.addMatch("type='signal',interface='dev.goose.test.FdPassing',member='MultiFdSignal'");
        try conn.registerSignalHandler("dev.goose.test.FdPassing", "MultiFdSignal", MultiFdTracker.callback, &tracker);

        var pipe_a: [2]std.posix.fd_t = undefined;
        var pipe_b: [2]std.posix.fd_t = undefined;
        _ = std.posix.system.pipe(&pipe_a);
        _ = std.posix.system.pipe(&pipe_b);
        defer _ = std.posix.system.close(pipe_a[1]);
        defer _ = std.posix.system.close(pipe_b[1]);

        _ = std.posix.system.write(pipe_a[1], "multi-fd payload A".ptr, 18);
        _ = std.posix.system.write(pipe_b[1], "multi-fd payload B".ptr, 18);

        const PairType = struct { a: GUFd, b: GUFd };
        var encoder = try message.BodyEncoder.encode(allocator, PairType{ .a = GUFd.new(0), .b = GUFd.new(1) });
        defer encoder.deinit();

        const serial = conn.nextSerial();
        const header = goose.core.MessageHeader{
            .message_type = .Signal,
            .flags = 0,
            .proto_version = 1,
            .body_length = @intCast(encoder.body().len),
            .serial = serial,
            .header_fields = @constCast(&[_]goose.core.HeaderField{
                .{ .code = .Path, .value = .{ .Path = "/dev/goose/test/FdPassing" } },
                .{ .code = .Interface, .value = .{ .Interface = "dev.goose.test.FdPassing" } },
                .{ .code = .Member, .value = .{ .Member = "MultiFdSignal" } },
                .{ .code = .Signature, .value = .{ .Signature = encoder.signature() } },
            }),
        };

        const fds_to_send = [_]std.posix.fd_t{ pipe_a[0], pipe_b[0] };
        const msg = goose.core.Message.newWithFds(header, encoder.body(), &fds_to_send);
        try conn.sendMessage(msg);
        _ = std.posix.system.close(pipe_a[0]);
        _ = std.posix.system.close(pipe_b[0]);

        var attempts: usize = 0;
        while (attempts < 100) : (attempts += 1) {
            if (tracker.received.load(.acquire) and tracker.matched_a.load(.acquire) and tracker.matched_b.load(.acquire)) break;
            var req = std.os.linux.timespec{ .sec = 0, .nsec = 10 * std.time.ns_per_ms };
            _ = std.os.linux.nanosleep(&req, null);
        }

        if (!tracker.received.load(.acquire) or tracker.count.load(.acquire) != 2) {
            std.debug.print("FAIL: Expected 2 file descriptors, got {d}\n", .{tracker.count.load(.acquire)});
            return error.MultiFdFailed;
        }
        if (!tracker.matched_a.load(.acquire) or !tracker.matched_b.load(.acquire)) {
            std.debug.print("FAIL: Multi-FD payloads did not match expected values!\n", .{});
            return error.MultiFdPayloadMismatch;
        }
        std.debug.print("PASS: Received 2 distinct file descriptors and validated respective payloads.\n", .{});
    }

    // -------------------------------------------------------------
    // Test 3: Auto-Close of Untaken Descriptors in freeMessage
    // -------------------------------------------------------------
    std.debug.print("\n--- Subtest 3: Untaken FDs Automatically Closed by freeMessage ---\n", .{});
    {
        const AutoCloseTracker = struct {
            recorded_fd: std.atomic.Value(std.posix.fd_t) = .init(-1),
            done: std.atomic.Value(bool) = .init(false),

            fn callback(ctx_ptr: ?*anyopaque, msg: goose.core.Message) void {
                const self: *@This() = @ptrCast(@alignCast(ctx_ptr.?));
                if (msg.fds.len == 0) return;
                self.recorded_fd.store(msg.fds[0], .release);
                self.done.store(true, .release);
                // Intentionally do NOT call takeFds()! freeMessage must close it.
            }
        };

        var tracker = AutoCloseTracker{};
        try conn.addMatch("type='signal',interface='dev.goose.test.FdPassing',member='AutoCloseSignal'");
        try conn.registerSignalHandler("dev.goose.test.FdPassing", "AutoCloseSignal", AutoCloseTracker.callback, &tracker);

        var pipe_fds: [2]std.posix.fd_t = undefined;
        _ = std.posix.system.pipe(&pipe_fds);
        defer _ = std.posix.system.close(pipe_fds[1]);

        var encoder = try message.BodyEncoder.encode(allocator, GUFd.new(0));
        defer encoder.deinit();

        const serial = conn.nextSerial();
        const header = goose.core.MessageHeader{
            .message_type = .Signal,
            .flags = 0,
            .proto_version = 1,
            .body_length = @intCast(encoder.body().len),
            .serial = serial,
            .header_fields = @constCast(&[_]goose.core.HeaderField{
                .{ .code = .Path, .value = .{ .Path = "/dev/goose/test/FdPassing" } },
                .{ .code = .Interface, .value = .{ .Interface = "dev.goose.test.FdPassing" } },
                .{ .code = .Member, .value = .{ .Member = "AutoCloseSignal" } },
                .{ .code = .Signature, .value = .{ .Signature = encoder.signature() } },
            }),
        };

        const fds_to_send = [_]std.posix.fd_t{pipe_fds[0]};
        const msg = goose.core.Message.newWithFds(header, encoder.body(), &fds_to_send);
        try conn.sendMessage(msg);
        _ = std.posix.system.close(pipe_fds[0]);

        var attempts: usize = 0;
        while (attempts < 100) : (attempts += 1) {
            if (tracker.done.load(.acquire)) break;
            var req = std.os.linux.timespec{ .sec = 0, .nsec = 10 * std.time.ns_per_ms };
            _ = std.os.linux.nanosleep(&req, null);
        }

        // Give dispatchLoop a tiny slice to finish dispatchUnsolicited and defer freeMessage
        var req = std.os.linux.timespec{ .sec = 0, .nsec = 20 * std.time.ns_per_ms };
        _ = std.os.linux.nanosleep(&req, null);

        const closed_fd = tracker.recorded_fd.load(.acquire);
        if (closed_fd < 0) return error.AutoCloseSignalNotReceived;

        // Reading from closed_fd must fail with EBADF (file descriptor closed)
        var dummy_buf: [1]u8 = undefined;
        const read_res = std.posix.read(closed_fd, &dummy_buf);
        if (read_res) |_| {
            std.debug.print("FAIL: Untaken FD {d} remained open after freeMessage!\n", .{closed_fd});
            return error.UntakenFdNotClosed;
        } else |err| {
            std.debug.print("PASS: Untaken FD ({d}) was cleanly closed ({any}).\n", .{ closed_fd, err });
        }
    }

    std.debug.print("\nAll UNIX FD passing tests passed successfully!\n", .{});
}
