const std = @import("std");
const goose = @import("goose");
const Connection = goose.Connection;
const message = goose.message;
const GUFd = goose.GUFd;
const ResolvedFd = goose.ResolvedFd;
const GStr = goose.core.value.GStr;

fn sleep(ms: i64) void {
    var req = std.posix.timespec{ .sec = 0, .nsec = ms * std.time.ns_per_ms };
    _ = std.os.linux.nanosleep(&req, null);
}

pub fn main(i: std.process.Init) !void {
    const allocator = i.gpa;

    std.debug.print("=== Test Suite: Out-of-Band UNIX File Descriptor Transport (SCM_RIGHTS) ===\n", .{});

    var conn = try Connection.init(allocator, .Session, i.io, i.environ_map);
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
            sleep(10);
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
            sleep(10);
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
            sleep(10);
        }

        // Give dispatchLoop a tiny slice to finish dispatchUnsolicited and defer freeMessage
        sleep(20);

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

    // -------------------------------------------------------------
    // Test 4: ResolvedFd and []ResolvedFd Decoding and duplicate()
    // -------------------------------------------------------------
    std.debug.print("\n--- Subtest 4: ResolvedFd and []ResolvedFd Decoding and duplicate() ---\n", .{});
    {
        // 4.1 Single ResolvedFd decoding & duplicate()
        var p1: [2]std.posix.fd_t = undefined;
        _ = std.posix.system.pipe(&p1);
        defer _ = std.posix.system.close(p1[1]);

        _ = std.posix.system.write(p1[1], "resolved-fd-single", 18);

        var enc1 = try message.BodyEncoder.encode(allocator, GUFd.new(0));
        defer enc1.deinit();

        var dec1 = message.BodyDecoder.initWithFds(allocator, enc1.body(), enc1.signature(), .little, &.{p1[0]});
        const rfd = try dec1.decode(ResolvedFd);
        if (rfd.index != 0 or rfd.handle != p1[0]) return error.ResolvedFdMismatch;

        // Duplicate the descriptor
        const dup_fd = try rfd.duplicate();
        defer _ = std.posix.system.close(dup_fd);

        if (dup_fd == rfd.handle) return error.DuplicateSameHandle;

        // Close original read handle; duplicate must remain valid
        _ = std.posix.system.close(p1[0]);

        var read_buf: [32]u8 = undefined;
        const n = try std.posix.read(dup_fd, &read_buf);
        if (!std.mem.eql(u8, read_buf[0..n], "resolved-fd-single")) {
            return error.PayloadMismatchAfterDup;
        }

        // Verify FD_CLOEXEC is set on duplicated handle
        const flags = std.posix.system.fcntl(dup_fd, std.posix.F.GETFD, 0);
        if ((flags & std.posix.FD_CLOEXEC) == 0) return error.CloexecNotSet;

        std.debug.print("PASS: ResolvedFd decoded and duplicate() preserved data with O_CLOEXEC.\n", .{});

        // 4.2 Array of ResolvedFd ([]ResolvedFd) decoding
        var pa: [2]std.posix.fd_t = undefined;
        var pb: [2]std.posix.fd_t = undefined;
        _ = std.posix.system.pipe(&pa);
        _ = std.posix.system.pipe(&pb);
        defer _ = std.posix.system.close(pa[1]);
        defer _ = std.posix.system.close(pb[1]);
        defer _ = std.posix.system.close(pa[0]);
        defer _ = std.posix.system.close(pb[0]);

        _ = std.posix.system.write(pa[1], "elem-a", 6);
        _ = std.posix.system.write(pb[1], "elem-b", 6);

        const rfds_to_encode = [_]ResolvedFd{
            ResolvedFd.init(pa[0], 0),
            ResolvedFd.init(pb[0], 1),
        };
        var enc_arr = try message.BodyEncoder.encode(allocator, &rfds_to_encode);
        defer enc_arr.deinit();

        var dec_arr = message.BodyDecoder.initWithFds(allocator, enc_arr.body(), enc_arr.signature(), .little, &.{ pa[0], pb[0] });
        const decoded_slice = try dec_arr.decode([]ResolvedFd);
        defer allocator.free(decoded_slice);

        if (decoded_slice.len != 2) return error.ArrayLenMismatch;
        if (decoded_slice[0].handle != pa[0] or decoded_slice[1].handle != pb[0]) return error.ArrayHandleMismatch;

        var buf_a: [16]u8 = undefined;
        var buf_b: [16]u8 = undefined;
        const na = try std.posix.read(decoded_slice[0].handle, &buf_a);
        const nb = try std.posix.read(decoded_slice[1].handle, &buf_b);
        if (!std.mem.eql(u8, buf_a[0..na], "elem-a") or !std.mem.eql(u8, buf_b[0..nb], "elem-b")) {
            return error.ArrayPayloadMismatch;
        }

        std.debug.print("PASS: []ResolvedFd decoded successfully with multiple handles.\n", .{});
    }

    // -------------------------------------------------------------
    // Test 5: D-Bus Object Method Dispatch with ResolvedFd and []ResolvedFd
    // -------------------------------------------------------------
    std.debug.print("\n--- Subtest 5: Object Method Dispatch with ResolvedFd & []ResolvedFd ---\n", .{});
    {
        const FdServiceTracker = struct {
            received_text: [64]u8 = undefined,
            received_len: usize = 0,
            duplicated_fd: std.posix.fd_t = -1,
            list_total_bytes: usize = 0,
            single_done: std.atomic.Value(bool) = .init(false),
            list_done: std.atomic.Value(bool) = .init(false),
        };

        const FdReceiverService = struct {
            pub const INTERFACE_NAME = "dev.goose.test.FdReceiver";

            tracker: *FdServiceTracker,

            pub fn init(_: *Connection, tracker: *FdServiceTracker) @This() {
                return .{ .tracker = tracker };
            }

            pub fn ReceiveFd(self: *@This(), label: GStr, rfd: ResolvedFd) !u32 {
                _ = label;
                var buf: [64]u8 = undefined;
                const n = try std.posix.read(rfd.handle, &buf);
                @memcpy(self.tracker.received_text[0..n], buf[0..n]);
                self.tracker.received_len = n;

                // Duplicate descriptor to persist beyond method lifetime
                self.tracker.duplicated_fd = try rfd.duplicate();
                self.tracker.single_done.store(true, .release);
                return 42;
            }

            pub fn ReceiveFdList(self: *@This(), rfds: []ResolvedFd) !u32 {
                var total: usize = 0;
                for (rfds) |rfd| {
                    var buf: [32]u8 = undefined;
                    const n = try std.posix.read(rfd.handle, &buf);
                    total += n;
                }
                self.tracker.list_total_bytes = total;
                self.tracker.list_done.store(true, .release);
                return @intCast(total);
            }
        };

        var tracker = FdServiceTracker{};
        try conn.registerObject(FdReceiverService, "dev.goose.test.FdReceiver", "/dev/goose/test/FdReceiver", &tracker);

        // Introspection verification
        var intro_reply = try conn.methodCall(
            "dev.goose.test.FdReceiver",
            "/dev/goose/test/FdReceiver",
            "org.freedesktop.DBus.Introspectable",
            "Introspect",
            null,
            "",
        );
        defer conn.freeMessage(&intro_reply);

        var intro_dec = message.BodyDecoder.fromMessage(allocator, intro_reply);
        const intro_str = try intro_dec.decode(GStr);
        if (std.mem.indexOf(u8, intro_str.s, "<arg name=\"arg1\" type=\"h\" direction=\"in\"/>") == null) {
            std.debug.print("FAIL: Introspection XML missing ResolvedFd 'h' argument!\n{s}\n", .{intro_str.s});
            return error.IntrospectMissingFdArg;
        }
        if (std.mem.indexOf(u8, intro_str.s, "<arg name=\"arg0\" type=\"ah\" direction=\"in\"/>") == null) {
            std.debug.print("FAIL: Introspection XML missing []ResolvedFd 'ah' argument!\n{s}\n", .{intro_str.s});
            return error.IntrospectMissingFdListArg;
        }
        std.debug.print("PASS: Object Introspection XML validates 'h' and 'ah' signatures.\n", .{});

        // 5.1 Call ReceiveFd(label: GStr, rfd: ResolvedFd)
        var p_call: [2]std.posix.fd_t = undefined;
        _ = std.posix.system.pipe(&p_call);
        defer _ = std.posix.system.close(p_call[1]);

        _ = std.posix.system.write(p_call[1], "dispatch-fd-payload", 19);

        var call_enc = try message.BodyEncoder.encode(allocator, .{ GStr.new("upload"), ResolvedFd.init(p_call[0], 0) });
        defer call_enc.deinit();

        const call_fds = [_]std.posix.fd_t{p_call[0]};
        var reply = try conn.methodCallWithFds(
            "dev.goose.test.FdReceiver",
            "/dev/goose/test/FdReceiver",
            "dev.goose.test.FdReceiver",
            "ReceiveFd",
            call_enc.signature(),
            call_enc.body(),
            &call_fds,
        );
        defer conn.freeMessage(&reply);
        _ = std.posix.system.close(p_call[0]);

        var reply_dec = message.BodyDecoder.fromMessage(allocator, reply);
        const ret_val = try reply_dec.decode(u32);
        if (ret_val != 42) return error.InvalidMethodReturn;

        if (!tracker.single_done.load(.acquire)) return error.MethodNotExecuted;
        if (!std.mem.eql(u8, tracker.received_text[0..tracker.received_len], "dispatch-fd-payload")) {
            return error.DispatchedPayloadMismatch;
        }

        // Verify the duplicated descriptor is still alive after method return and original fd closure
        const dup_handle = tracker.duplicated_fd;
        if (dup_handle < 0) return error.DuplicatedFdInvalid;
        defer _ = std.posix.system.close(dup_handle);

        _ = std.posix.system.write(p_call[1], "+more-data", 10);
        var extra_buf: [16]u8 = undefined;
        const n_extra = try std.posix.read(dup_handle, &extra_buf);
        if (!std.mem.eql(u8, extra_buf[0..n_extra], "+more-data")) {
            return error.DuplicatedFdReadMismatch;
        }
        std.debug.print("PASS: ReceiveFd executed, returned 42, and duplicated FD outlived method call.\n", .{});

        // 5.2 Call ReceiveFdList(rfds: []ResolvedFd)
        var p_list1: [2]std.posix.fd_t = undefined;
        var p_list2: [2]std.posix.fd_t = undefined;
        _ = std.posix.system.pipe(&p_list1);
        _ = std.posix.system.pipe(&p_list2);
        defer _ = std.posix.system.close(p_list1[1]);
        defer _ = std.posix.system.close(p_list2[1]);

        _ = std.posix.system.write(p_list1[1], "12345", 5);
        _ = std.posix.system.write(p_list2[1], "67890!", 6);

        const list_args = [_]ResolvedFd{
            ResolvedFd.init(p_list1[0], 0),
            ResolvedFd.init(p_list2[0], 1),
        };
        var list_enc = try message.BodyEncoder.encode(allocator, .{list_args[0..]});
        defer list_enc.deinit();

        const list_fds = [_]std.posix.fd_t{ p_list1[0], p_list2[0] };
        var list_reply = try conn.methodCallWithFds(
            "dev.goose.test.FdReceiver",
            "/dev/goose/test/FdReceiver",
            "dev.goose.test.FdReceiver",
            "ReceiveFdList",
            list_enc.signature(),
            list_enc.body(),
            &list_fds,
        );
        defer conn.freeMessage(&list_reply);
        _ = std.posix.system.close(p_list1[0]);
        _ = std.posix.system.close(p_list2[0]);

        var list_reply_dec = message.BodyDecoder.fromMessage(allocator, list_reply);
        const list_ret = try list_reply_dec.decode(u32);
        if (list_ret != 11) {
            std.debug.print("FAIL: Expected total 11 bytes, got {d}\n", .{list_ret});
            return error.InvalidListReturn;
        }

        if (!tracker.list_done.load(.acquire)) return error.ListMethodNotExecuted;
        if (tracker.list_total_bytes != 11) return error.ListTotalMismatch;
        std.debug.print("PASS: ReceiveFdList executed successfully and read from multiple passed FDs.\n", .{});
    }

    std.debug.print("\nAll UNIX FD passing tests passed successfully!\n", .{});
}
