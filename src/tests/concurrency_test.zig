const std = @import("std");
const goose = @import("goose");
const Connection = goose.Connection;
const message = goose.message;
const GStr = goose.core.value.GStr;

fn concurrentCaller(conn: *Connection, allocator: std.mem.Allocator, expected_id: []const u8, err_flag: *std.atomic.Value(bool)) void {
    for (0..20) |_| {
        var reply = conn.methodCall(
            "org.freedesktop.DBus",
            "/org/freedesktop/DBus",
            "org.freedesktop.DBus",
            "GetId",
            null,
            &.{},
        ) catch {
            err_flag.store(true, .release);
            return;
        };
        defer conn.freeMessage(&reply);

        var decoder = message.BodyDecoder.fromMessage(allocator, reply);
        const id = decoder.decode(GStr) catch {
            err_flag.store(true, .release);
            return;
        };
        if (!std.mem.eql(u8, id.s, expected_id)) {
            err_flag.store(true, .release);
            return;
        }
    }
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    std.debug.print("=== Test 1: Concurrent Method Calls ===\n", .{});
    {
        var conn = try Connection.init(allocator, .Session, init.io, init.environ_map);
        defer conn.close();

        // First get reference ID
        var ref_reply = try conn.methodCall(
            "org.freedesktop.DBus",
            "/org/freedesktop/DBus",
            "org.freedesktop.DBus",
            "GetId",
            null,
            &.{},
        );
        defer conn.freeMessage(&ref_reply);
        var ref_dec = message.BodyDecoder.fromMessage(allocator, ref_reply);
        const ref_id = try ref_dec.decode(GStr);
        std.debug.print("Reference Bus ID: {s}\n", .{ref_id.s});

        const thread_count = 6;
        var threads: [thread_count]std.Thread = undefined;
        var err_flag = std.atomic.Value(bool).init(false);

        for (0..thread_count) |i| {
            threads[i] = try std.Thread.spawn(.{}, concurrentCaller, .{ &conn, allocator, ref_id.s, &err_flag });
        }

        for (0..thread_count) |i| {
            threads[i].join();
        }

        if (err_flag.load(.acquire)) {
            std.debug.print("FAIL: Concurrent calls encountered an error or mismatch!\n", .{});
            return error.TestFailed;
        }
        std.debug.print("PASS: Concurrent calls executed successfully without corruption.\n", .{});
    }

    std.debug.print("\n=== Test 2: Orderly serve() Shutdown ===\n", .{});
    {
        var conn = try Connection.init(allocator, .Session, init.io, init.environ_map);

        const ServerRunner = struct {
            fn run(c: *Connection) void {
                c.serve() catch {};
            }
        };

        const server_thread = try std.Thread.spawn(.{}, ServerRunner.run, .{&conn});

        // Let the server thread enter serve()
        var req = std.os.linux.timespec{ .sec = 0, .nsec = 100 * std.time.ns_per_ms };
        _ = std.os.linux.nanosleep(&req, null);

        // Close from another thread
        std.debug.print("Closing connection from main thread...\n", .{});
        conn.close();

        // Server thread should wake up and terminate promptly
        server_thread.join();
        std.debug.print("PASS: serve() terminated cleanly on connection close.\n", .{});
    }

    std.debug.print("\n=== Test 3: In-Order Signal Dispatch ===\n", .{});
    {
        var conn = try Connection.init(allocator, .Session, init.io, init.environ_map);
        defer conn.close();

        const SignalTracker = struct {
            last_seq: u32 = 0,
            count: u32 = 0,
            in_order: bool = true,

            fn callback(ctx_ptr: ?*anyopaque, msg: goose.core.Message) void {
                const self: *@This() = @ptrCast(@alignCast(ctx_ptr.?));
                var dec = message.BodyDecoder.fromMessage(std.heap.page_allocator, msg);
                const seq = dec.decode(u32) catch return;
                if (seq != self.last_seq + 1) {
                    self.in_order = false;
                }
                self.last_seq = seq;
                self.count += 1;
            }
        };

        var tracker = SignalTracker{};
        try conn.addMatch("type='signal',interface='dev.goose.test.Signals'");
        try conn.registerSignalHandler("dev.goose.test.Signals", "SeqSignal", SignalTracker.callback, &tracker);

        const total_signals: u32 = 25;
        for (1..total_signals + 1) |seq| {
            var encoder = try message.BodyEncoder.encode(allocator, @as(u32, @intCast(seq)));
            defer encoder.deinit();

            const serial = conn.nextSerial();
            const header = goose.core.MessageHeader{
                .message_type = .Signal,
                .flags = 0,
                .proto_version = 1,
                .body_length = @intCast(encoder.body().len),
                .serial = serial,
                .header_fields = @constCast(&[_]goose.core.HeaderField{
                    .{ .code = .Path, .value = .{ .Path = "/dev/goose/test/Signals" } },
                    .{ .code = .Interface, .value = .{ .Interface = "dev.goose.test.Signals" } },
                    .{ .code = .Member, .value = .{ .Member = "SeqSignal" } },
                    .{ .code = .Signature, .value = .{ .Signature = encoder.signature() } },
                }),
            };
            try conn.sendMessage(goose.core.Message.new(header, encoder.body()));
        }

        // Wait up to 1 second for all signals to be processed by dispatchLoop
        var attempts: usize = 0;
        while (tracker.count < total_signals and attempts < 100) : (attempts += 1) {
            var req = std.os.linux.timespec{ .sec = 0, .nsec = 10 * std.time.ns_per_ms };
            _ = std.os.linux.nanosleep(&req, null);
        }

        if (tracker.count != total_signals) {
            std.debug.print("FAIL: Expected {d} signals, got {d}\n", .{ total_signals, tracker.count });
            return error.SignalCountMismatch;
        }
        if (!tracker.in_order) {
            std.debug.print("FAIL: Signals were received out of order!\n", .{});
            return error.SignalsOutOfOrder;
        }
        std.debug.print("PASS: Received {d}/{d} signals strictly in sequence.\n", .{ tracker.count, total_signals });
    }

    std.debug.print("\n=== Test 4: Single-Threaded Poll Backend ===\n", .{});
    {
        var conn = try Connection.initWithBackend(allocator, .Session, init.io, init.environ_map, .poll);
        defer conn.close();

        if (conn.backend != .poll) return error.BackendMismatch;
        const fd = conn.getFd();
        if (fd < 0) return error.InvalidFd;

        if (conn.worker_thread != null or conn.dispatch_thread != null) {
            return error.ThreadSpawnedUnexpectedly;
        }

        if (conn.serve()) |_| {
            return error.ServeShouldFailInPollBackend;
        } else |err| {
            if (err != error.InvalidBackend) return err;
        }

        var reply = try conn.methodCall(
            "org.freedesktop.DBus",
            "/org/freedesktop/DBus",
            "org.freedesktop.DBus",
            "GetId",
            null,
            &.{},
        );
        defer conn.freeMessage(&reply);

        var dec = message.BodyDecoder.fromMessage(allocator, reply);
        const bus_id = try dec.decode(GStr);
        std.debug.print("Poll Backend Bus ID: {s}\n", .{bus_id.s});

        if (conn.worker_thread != null or conn.dispatch_thread != null) {
            return error.ThreadSpawnedUnexpectedly;
        }

        const PollSignalTracker = struct {
            count: u32 = 0,
            last_seq: u32 = 0,

            fn callback(ctx_ptr: ?*anyopaque, msg: goose.core.Message) void {
                const self: *@This() = @ptrCast(@alignCast(ctx_ptr.?));
                var d = message.BodyDecoder.fromMessage(std.heap.page_allocator, msg);
                const seq = d.decode(u32) catch return;
                self.count += 1;
                self.last_seq = seq;
            }
        };

        var tracker = PollSignalTracker{};
        try conn.addMatch("type='signal',interface='dev.goose.test.PollSignals'");
        try conn.registerSignalHandler("dev.goose.test.PollSignals", "TestSig", PollSignalTracker.callback, &tracker);

        const test_val: u32 = 42;
        var encoder = try message.BodyEncoder.encode(allocator, test_val);
        defer encoder.deinit();

        const serial = conn.nextSerial();
        const header = goose.core.MessageHeader{
            .message_type = .Signal,
            .flags = 0,
            .proto_version = 1,
            .body_length = @intCast(encoder.body().len),
            .serial = serial,
            .header_fields = @constCast(&[_]goose.core.HeaderField{
                .{ .code = .Path, .value = .{ .Path = "/dev/goose/test/PollSignals" } },
                .{ .code = .Interface, .value = .{ .Interface = "dev.goose.test.PollSignals" } },
                .{ .code = .Member, .value = .{ .Member = "TestSig" } },
                .{ .code = .Signature, .value = .{ .Signature = encoder.signature() } },
            }),
        };
        try conn.sendMessage(goose.core.Message.new(header, encoder.body()));

        var pfd = [1]std.posix.pollfd{.{
            .fd = conn.getFd(),
            .events = std.posix.POLL.IN,
            .revents = 0,
        }};
        var polled = false;
        var retries: usize = 0;
        while (retries < 100) : (retries += 1) {
            _ = try std.posix.poll(&pfd, 10);
            if (pfd[0].revents & std.posix.POLL.IN != 0 or conn.hasDataToRead()) {
                while (try conn.dispatch()) {}
                if (tracker.count > 0) {
                    polled = true;
                    break;
                }
            }
        }

        if (!polled or tracker.count != 1 or tracker.last_seq != test_val) {
            std.debug.print("FAIL: Expected 1 signal with val {d}, got count={d}, val={d}\n", .{ test_val, tracker.count, tracker.last_seq });
            return error.PollDispatchFailed;
        }

        const has_more = try conn.dispatch();
        if (has_more) {
            return error.UnexpectedData;
        }

        std.debug.print("PASS: Single-threaded poll backend dispatched signal without threads.\n", .{});
    }

    std.debug.print("\nAll concurrency and lifecycle tests passed!\n", .{});
}
