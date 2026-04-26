using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using CH.Native.Protocol.Messages;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// Drives the post-handshake parsing path with precisely-shaped garbage that a
/// real ClickHouse server would never send. Backed by <see cref="MockClickHouseServer"/>;
/// Toxiproxy can shape the network but can't manufacture exact byte sequences,
/// which is what these tests need.
///
/// <para>
/// Each test owns its own mock server (no shared collection) so the scripted byte
/// stream is per-test and tests can run in parallel.
/// </para>
/// </summary>
[Trait(Categories.Name, Categories.Streams)]
public class MalformedServerTests
{
    private static readonly TimeSpan AntiHangTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task Server_ReturnsUnknownMessageType_TearsDownConnectionCleanly()
    {
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        // 0xFF is not a valid ServerMessageType. The pump must surface this as
        // ClickHouseProtocolException and poison the connection — the regression
        // we're guarding is silent loop/deadlock when the switch falls through.
        // VarInt-encoded as a single byte (top bit set) the value would be ill-formed,
        // but a plain 0xFF still decodes as a 7-bit value with a continuation bit;
        // pair it with 0x01 so the VarInt parses cleanly to 0xFF (255).
        mock.EnqueueBytes(new byte[] { 0xFF, 0x01 });

        // QueryAsync returns IAsyncEnumerable — drive enumeration inside a Task
        // so we can race the result against an anti-hang timeout. Don't trust the
        // library to honour cancellation here (covered by M6); we just want a
        // regression deadlock to fail the test instead of hanging the run.
        var query = Task.Run(async () =>
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1")) { }
        });

        var winner = await Task.WhenAny(query, Task.Delay(AntiHangTimeout));
        Assert.Same(query, winner);

        var ex = await Assert.ThrowsAsync<ClickHouseProtocolException>(() => query);
        Assert.Contains("Unknown server message type", ex.Message);
        Assert.False(conn.CanBePooled, "connection must be poisoned after a protocol violation");
    }

    [Fact]
    public async Task Server_SendsOversizedStringLength_TearsDownBeforeAllocating()
    {
        // Connection is configured with a tiny MaxStringLength (1 KiB); the mock
        // sends a Data message whose first string (the table name) declares a
        // 64 MiB length. The library must reject at the length-prefix check —
        // never allocate the 64 MiB buffer — and tear the connection down.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings(b =>
            b.WithMaxStringLength(1024)));
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        // Data message-type byte, then a length-prefix of 64 MiB and zero payload.
        // We never fill the payload — that's the point: the cap fires first.
        var payload = ProtocolByteBuilder_BuildOversizedStringMessage(declaredLengthBytes: 64 * 1024 * 1024);
        mock.EnqueueBytes(payload);

        var query = Task.Run(async () =>
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1")) { }
        });

        var winner = await Task.WhenAny(query, Task.Delay(AntiHangTimeout));
        Assert.Same(query, winner);

        var ex = await Assert.ThrowsAsync<ClickHouseProtocolException>(() => query);
        Assert.Contains("MaxStringLengthBytes", ex.Message);
        Assert.False(conn.CanBePooled, "oversized string must poison the connection");
    }

    private static byte[] ProtocolByteBuilder_BuildOversizedStringMessage(int declaredLengthBytes)
    {
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Data);
        w.WriteVarInt((ulong)declaredLengthBytes);
        // No payload — caller asserts we throw before consuming it.
        return bw.WrittenMemory.ToArray();
    }

    [Fact]
    public async Task Server_SendsExtraBytesAfterEndOfStream_PinnedAsFatal()
    {
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        // Compose a minimal valid response: an empty Data block (header indicating
        // 0 columns / 0 rows) followed by EndOfStream, then 64 bytes of junk that
        // the server has no business sending. ClickHouse's data-block format is:
        //   varint(messageType=Data=1)
        //   string(table_name)            -- empty
        //   block_info: varint(field_num=1) varint(is_overflows=0)
        //                varint(field_num=2) varint(bucket_num=-1 as int32 LE)
        //                varint(field_num=0)  -- terminator
        //   varint(num_columns=0)
        //   varint(num_rows=0)
        // Then EndOfStream is just varint(messageType=5).
        // Followed by 64 bytes of garbage to trigger the L4 trailing-byte check.
        var payload = BuildEmptyBlockPlusEosPlusJunk(junkBytes: 64);
        mock.EnqueueBytes(payload);
        mock.CompleteOutgoing();

        var query = Task.Run(async () =>
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1")) { }
        });

        var winner = await Task.WhenAny(query, Task.Delay(AntiHangTimeout));
        Assert.Same(query, winner);

        var ex = await Assert.ThrowsAsync<ClickHouseProtocolException>(() => query);
        Assert.Contains("after EndOfStream", ex.Message);
        Assert.False(conn.CanBePooled, "post-EOS extra bytes must poison the connection");
    }

    [Fact]
    public async Task Server_SendsValidExceptionMessage_ConnectionRemainsUsable()
    {
        // M3: a complete, well-formed Exception followed by EndOfStream is the
        // happy-path failure mode. The query throws ClickHouseServerException with
        // the wire fields surfaced; the connection stays usable for the next query
        // (no protocol-fatal flag set, since the bytes were structurally fine).
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        var payload = BuildExceptionMessageBytes(
            errorCode: 241,
            name: "DB::Exception",
            message: "memory limit exceeded",
            stackTrace: "stack here",
            hasNested: false);
        mock.EnqueueBytes(payload);

        ClickHouseServerException? thrown = null;
        try
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1")) { }
        }
        catch (ClickHouseServerException ex) { thrown = ex; }

        Assert.NotNull(thrown);
        Assert.Equal(241, thrown!.ErrorCode);
        Assert.Contains("memory limit", thrown.Message);
        Assert.True(conn.CanBePooled, "well-formed server exception must NOT poison the connection");
    }

    [Fact]
    public async Task Server_StreamsBlocks_ThenSendsExceptionMidStream_ConnectionUsable()
    {
        // M4: server sends a few normal data blocks, then an Exception, then EOS.
        // The reader yields the row counts from the blocks first, then the next
        // iteration step throws ClickHouseServerException. The connection remains
        // usable because the bytes were structurally fine — only logically failed.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        var payload = BuildEmptyBlocksPlusException(blockCount: 3);
        mock.EnqueueBytes(payload);

        int blocksObserved = 0;
        ClickHouseServerException? thrown = null;
        try
        {
            await foreach (var row in conn.QueryAsync<int>("SELECT 1"))
            {
                blocksObserved++;
            }
        }
        catch (ClickHouseServerException ex) { thrown = ex; }

        Assert.NotNull(thrown);
        Assert.Equal(0, blocksObserved); // empty blocks contain no rows
        Assert.True(conn.CanBePooled, "mid-stream server exception must NOT poison the connection");
    }

    [Fact]
    public async Task Server_HappyPathSinglePacket_QuerySucceeds()
    {
        // Sanity check: the same bytes as the dribble test but in a single chunk.
        // If THIS fails the dribble test failure isn't really about fragmentation —
        // it's about the payload itself being wrong. Keep this test alongside the
        // dribble variant so a future regression can be triaged quickly.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        var payload = BuildEmptyBlockPlusEosPlusJunk(junkBytes: 0);
        mock.EnqueueBytes(payload);
        mock.CompleteOutgoing();

        var query = Task.Run(async () =>
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1")) { }
        });

        var winner = await Task.WhenAny(query, Task.Delay(TimeSpan.FromSeconds(10)));
        Assert.Same(query, winner);
        await query;
    }

    [Fact]
    public async Task Server_DribblesBytes_OneAtATime_QuerySucceeds()
    {
        // M5: the same bytes as the happy-path response but transmitted one byte
        // at a time (with a flush between each). Validates that the pump's scan-
        // then-parse contract correctly returns "incomplete" on every partial read
        // without triggering a real failure. This is the regression test for L3.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        var payload = BuildEmptyBlockPlusEosPlusJunk(junkBytes: 0);
        // Push each byte individually so the pump's ReadAsync sees N tiny packets.
        // The mock flushes after every Send, so the pipe genuinely fragments.
        for (int i = 0; i < payload.Length; i++)
            mock.EnqueueBytes(payload.AsSpan(i, 1));
        mock.CompleteOutgoing();

        var query = Task.Run(async () =>
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1")) { }
        });

        var winner = await Task.WhenAny(query, Task.Delay(TimeSpan.FromSeconds(10)));
        Assert.Same(query, winner);
        await query; // surface any exception
    }

    [Fact]
    public async Task Server_HangsAfterPartialMessage_RespectsCancellationToken()
    {
        // M6: server sends half an Exception message and then never more. Caller
        // cancels via CancellationToken — the pump must surface OperationCanceledException
        // promptly rather than hanging waiting for the rest of the message.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        // Type byte + half the int32 code, no more.
        var partial = ProtocolByteBuilder_BuildPartialException();
        mock.EnqueueBytes(partial);
        // Deliberately do NOT call CompleteOutgoing — the connection stays open
        // with the client waiting for more bytes that will never arrive.

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        OperationCanceledException? caught = null;
        try
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1").WithCancellation(cts.Token)) { }
        }
        catch (OperationCanceledException ex) { caught = ex; }

        Assert.NotNull(caught);
    }

    private static byte[] BuildExceptionMessageBytes(int errorCode, string name, string message, string stackTrace, bool hasNested)
    {
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Exception);
        w.WriteInt32(errorCode);
        w.WriteString(name);
        w.WriteString(message);
        w.WriteString(stackTrace);
        w.WriteByte(hasNested ? (byte)1 : (byte)0);
        return bw.WrittenMemory.ToArray();
    }

    private static byte[] BuildEmptyBlocksPlusException(int blockCount)
    {
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        for (int i = 0; i < blockCount; i++)
        {
            w.WriteVarInt((ulong)Protocol.ServerMessageType.Data);
            w.WriteString(string.Empty);
            BlockInfo.Default.Write(ref w);
            w.WriteVarInt(0);
            w.WriteVarInt(0);
        }
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Exception);
        w.WriteInt32(241);
        w.WriteString("DB::Exception");
        w.WriteString("mid-stream failure");
        w.WriteString("stack");
        w.WriteByte(0);
        return bw.WrittenMemory.ToArray();
    }

    private static byte[] ProtocolByteBuilder_BuildPartialException()
    {
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Exception);
        // Two of the four code bytes — deliberately incomplete.
        w.WriteByte(0x01);
        w.WriteByte(0x02);
        return bw.WrittenMemory.ToArray();
    }

    [Fact]
    public async Task ConnectionReusableAfterEos_SecondQuerySucceeds()
    {
        // Pipe-state regression test: the L4 trailing-byte bug was a false-positive
        // that misidentified the EOS byte itself as "trailing". The dribble test (M5)
        // caught the false-positive, but a different class of bug — leaving genuine
        // unread bytes in the pipe between queries — would only surface when the
        // *next* query runs. This test pins that: after a clean Data+EOS round, a
        // second scripted Data+EOS round on the same connection must succeed.
        //
        // If the pipe leaks state (un-AdvanceTo'd bytes, mis-positioned reader),
        // the second query parses garbage and throws ClickHouseProtocolException.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        // Query 1 response — drained by the first foreach.
        mock.EnqueueBytes(BuildEmptyBlockPlusEosPlusJunk(junkBytes: 0));
        await foreach (var _ in conn.QueryAsync<int>("SELECT 1")) { }

        Assert.True(conn.CanBePooled, "after a clean EOS the connection must remain reusable");

        // Query 2 response — must be picked up by a second QueryAsync against the
        // same connection. If the pipe still held un-AdvanceTo'd bytes from query 1,
        // this would either deadlock (bytes the parser is waiting for never arrive)
        // or parse garbage (stale bytes prefix the new response).
        mock.EnqueueBytes(BuildEmptyBlockPlusEosPlusJunk(junkBytes: 0));

        var query2 = Task.Run(async () =>
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 2")) { }
        });
        var winner = await Task.WhenAny(query2, Task.Delay(AntiHangTimeout));
        Assert.Same(query2, winner);
        await query2;

        Assert.True(conn.CanBePooled, "second clean query must also leave the connection usable");
    }

    [Fact]
    public async Task JunkArrivingAfterEos_OnNextQuery_FailsViaUnknownMessageTypeDefence()
    {
        // Pins the L1 backstop for trailing bytes that arrive in a separate packet
        // after EOS. L4 only checks bytes in the same buffer as EOS — anything later
        // gets caught when the next query's pump reads it as a fresh message-type
        // byte (which won't decode to any known ServerMessageType).
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        // First query: clean response.
        mock.EnqueueBytes(BuildEmptyBlockPlusEosPlusJunk(junkBytes: 0));
        await foreach (var _ in conn.QueryAsync<int>("SELECT 1")) { }
        Assert.True(conn.CanBePooled, "clean first query must not poison");

        // Inject 16 bytes of junk that the server has no business sending. They
        // arrive in a fresh packet (separate from the EOS), so L4 doesn't fire.
        // The next QueryAsync's pump will read them as if they were a new message
        // and L1 must trip on the unknown type byte.
        mock.EnqueueBytes(new byte[] { 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10,
                                       0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10 });

        var query2 = Task.Run(async () =>
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 2")) { }
        });
        var winner = await Task.WhenAny(query2, Task.Delay(AntiHangTimeout));
        Assert.Same(query2, winner);

        var ex = await Assert.ThrowsAsync<ClickHouseProtocolException>(() => query2);
        // Either L1 ("Unknown server message type") or L4 ("after EndOfStream") is
        // an acceptable defence depending on exact pump timing — what matters is
        // the failure mode is *typed* and the connection is poisoned.
        Assert.True(
            ex.Message.Contains("Unknown server message type") ||
            ex.Message.Contains("after EndOfStream"),
            $"expected protocol exception from L1 or L4, got: {ex.Message}");
        Assert.False(conn.CanBePooled);
    }

    [Fact]
    public async Task BackToBackQueries_ResultsAndExceptionsLandOnCorrectQuery()
    {
        // Three sequential queries on the same connection with mixed outcomes:
        //   q1: clean Data + EOS                          (success)
        //   q2: server Exception + EOS                    (throws ClickHouseServerException)
        //   q3: clean Data + EOS                          (success)
        //
        // Pins that exception-on-q2 doesn't poison the pipe state for q3, and that
        // result framing for each query lands on the correct iterator. A regression
        // here would surface as q3 throwing q2's exception (or seeing q2's bytes),
        // which would be a much harder bug to debug in production than a deadlock.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        // --- q1 ---
        mock.EnqueueBytes(BuildEmptyBlockPlusEosPlusJunk(junkBytes: 0));
        await foreach (var _ in conn.QueryAsync<int>("SELECT 1")) { }
        Assert.True(conn.CanBePooled);

        // --- q2 ---
        mock.EnqueueBytes(BuildExceptionMessageBytes(
            errorCode: 241, name: "DB::Exception", message: "q2 failure",
            stackTrace: "stack", hasNested: false));

        ClickHouseServerException? q2Ex = null;
        try
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 2")) { }
        }
        catch (ClickHouseServerException ex) { q2Ex = ex; }

        Assert.NotNull(q2Ex);
        Assert.Equal(241, q2Ex!.ErrorCode);
        Assert.Contains("q2 failure", q2Ex.Message);
        Assert.True(conn.CanBePooled, "well-formed server exception must not poison the connection");

        // --- q3 --- (proves q2's exception didn't leak state into q3's stream)
        mock.EnqueueBytes(BuildEmptyBlockPlusEosPlusJunk(junkBytes: 0));
        var q3 = Task.Run(async () =>
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 3")) { }
        });
        var winner = await Task.WhenAny(q3, Task.Delay(AntiHangTimeout));
        Assert.Same(q3, winner);
        await q3;
        Assert.True(conn.CanBePooled);
    }

    [Fact]
    public async Task Server_StreamsProgressInterleavedWithBlocks_ConnectionUsable()
    {
        // Pins that the dispatch correctly handles a realistic message-mix: a real
        // ClickHouse server interleaves Progress messages with Data blocks. If the
        // L3 scan-then-parse Progress branch consumes the wrong number of bytes,
        // the next message dispatch (Data or EOS) parses garbage. The most likely
        // regression here would be a Progress field added to ClickHouse without an
        // update to ProgressMessage.TryScan / Read — which would silently drift.
        //
        // We send: Data(empty) + Progress + Data(empty) + Progress + EOS.
        // The query must complete cleanly and the connection remain usable.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        var revision = MockClickHouseServer.PinnedProtocolRevision;
        var payload = BuildInterleavedDataAndProgress(revision, blockCount: 2, progressCount: 2);
        mock.EnqueueBytes(payload);
        mock.CompleteOutgoing();

        var query = Task.Run(async () =>
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1")) { }
        });

        var winner = await Task.WhenAny(query, Task.Delay(AntiHangTimeout));
        Assert.Same(query, winner);
        await query;
        Assert.True(conn.CanBePooled);
    }

    private static byte[] BuildInterleavedDataAndProgress(int revision, int blockCount, int progressCount)
    {
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);

        for (int i = 0; i < blockCount; i++)
        {
            // Empty Data block.
            w.WriteVarInt((ulong)Protocol.ServerMessageType.Data);
            w.WriteString(string.Empty);
            BlockInfo.Default.Write(ref w);
            w.WriteVarInt(0);
            w.WriteVarInt(0);

            if (i < progressCount)
            {
                // Progress message — fields gated by negotiated revision.
                w.WriteVarInt((ulong)Protocol.ServerMessageType.Progress);
                w.WriteVarInt(100);   // rows
                w.WriteVarInt(2048);  // bytes
                w.WriteVarInt(1000);  // totalRows
                if (revision >= ProtocolVersion.WithTotalBytesInProgress) w.WriteVarInt(20480);
                if (revision >= ProtocolVersion.WithClientWriteInfo) { w.WriteVarInt(0); w.WriteVarInt(0); }
                if (revision >= ProtocolVersion.WithServerQueryTimeInProgress) w.WriteVarInt(123456);
            }
        }

        w.WriteVarInt((ulong)Protocol.ServerMessageType.EndOfStream);
        return bw.WrittenMemory.ToArray();
    }

    private static byte[] BuildEmptyBlockPlusEosPlusJunk(int junkBytes)
    {
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);

        // --- empty Data message ---
        // Use the library's own Block.WriteEmpty so the wire shape stays in lockstep
        // with whatever block-format changes the library makes — the only thing we
        // need to swap is the leading message-type byte (Block.WriteEmpty writes the
        // *client* Data marker; the test needs the *server* one).
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Data);
        w.WriteString(string.Empty);            // table name
        BlockInfo.Default.Write(ref w);
        w.WriteVarInt(0);                       // num columns
        w.WriteVarInt(0);                       // num rows

        // --- EndOfStream ---
        w.WriteVarInt((ulong)Protocol.ServerMessageType.EndOfStream);

        // --- trailing junk ---
        var rng = new Random(0xC0FFEE);
        Span<byte> junk = stackalloc byte[64];
        rng.NextBytes(junk);
        w.WriteBytes(junk[..junkBytes]);

        return bw.WrittenMemory.ToArray();
    }
}
