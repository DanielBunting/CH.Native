using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// White-box tests for the bulk-insert response pump (<c>ReceiveEndOfStreamAsync</c>).
/// This is a separate pump from <c>ReadServerMessagesAsync</c> with its own dispatch
/// switch — the L1 (unknown-type defence), L3 (scan-then-parse), and L4 (post-EOS
/// trailing-byte) hardening from Gap 3 was originally scoped to the main pump only.
/// These tests pin the same invariants on the bulk-insert pump.
///
/// <para>
/// Drives the pump directly via <c>InternalsVisibleTo</c> rather than through a
/// real BulkInserter — the inserter exchanges schema bytes that would compete with
/// the test scripting. Calling the pump in isolation lets each test script the exact
/// post-INSERT response sequence.
/// </para>
/// </summary>
[Trait(Categories.Name, Categories.Streams)]
public class BulkInsertPumpTests
{
    private static readonly TimeSpan AntiHangTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task CleanEos_PumpReturnsCleanly_ConnectionUsable()
    {
        // Baseline: a single EOS byte must terminate the pump cleanly and leave
        // the connection in a usable state (not poisoned). Regression here would
        // be the pump throwing on the happy path.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(new byte[] { (byte)Protocol.ServerMessageType.EndOfStream });

        var pump = conn.ReceiveEndOfStreamAsync(CancellationToken.None);
        var winner = await Task.WhenAny(pump, Task.Delay(AntiHangTimeout));
        Assert.Same(pump, winner);
        await pump;

        Assert.True(conn.CanBePooled, "clean INSERT EOS must leave the connection usable");
    }

    [Fact]
    public async Task ServerException_TypedThrow_ConnectionUsable()
    {
        // The bulk-insert pump must surface a server-side exception as a typed
        // ClickHouseServerException (not a wrapped InvalidOperationException) and
        // must NOT poison the connection (the bytes were structurally fine — the
        // server just rejected the INSERT logically).
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(BuildExceptionPayload(
            errorCode: 252,
            name: "DB::Exception",
            message: "TOO_MANY_PARTS",
            stackTrace: "stack",
            hasNested: false));

        ClickHouseServerException? thrown = null;
        try { await conn.ReceiveEndOfStreamAsync(CancellationToken.None); }
        catch (ClickHouseServerException ex) { thrown = ex; }

        Assert.NotNull(thrown);
        Assert.Equal(252, thrown!.ErrorCode);
        Assert.Contains("TOO_MANY_PARTS", thrown.Message);
        Assert.True(conn.CanBePooled, "server-side INSERT failure must not poison the connection");
    }

    [Fact]
    public async Task UnknownMessageType_ThrowsProtocolExceptionAndPoisons()
    {
        // L1 extension to the bulk-insert pump. Pre-fix: an unknown type byte threw
        // a generic ClickHouseException with NO _protocolFatal set — the pool would
        // happily reuse a stream-misaligned connection. Post-fix: typed protocol
        // exception + connection poisoned.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        // VarInt-encode 0xFF as a 2-byte sequence so the first byte parses cleanly
        // as a continuation byte (0xFF & 0x7F = 0x7F with continuation bit) and
        // the second byte sets the high bits — yields VarInt 0xFF (255).
        mock.EnqueueBytes(new byte[] { 0xFF, 0x01 });

        var pump = Task.Run(() => conn.ReceiveEndOfStreamAsync(CancellationToken.None));
        var winner = await Task.WhenAny(pump, Task.Delay(AntiHangTimeout));
        Assert.Same(pump, winner);

        var ex = await Assert.ThrowsAsync<ClickHouseProtocolException>(() => pump);
        Assert.Contains("Unknown server message type", ex.Message);
        Assert.Contains("INSERT", ex.Message);
        Assert.False(conn.CanBePooled, "unknown message type during INSERT must poison the connection");
    }

    [Fact]
    public async Task DribblesBytes_OneAtATime_PumpCompletes()
    {
        // L3 extension to the bulk-insert pump: the scan-then-parse pattern lets
        // the pump cleanly handle byte-by-byte fragmentation of an Exception or
        // Progress message. Pre-fix the catch-IOException backstop happened to
        // work, but only because every Read* method was guaranteed to throw
        // InvalidOperationException on incomplete data. Post-fix the contract
        // is explicit. Drive an Exception message one byte at a time and assert
        // the typed exception lands on the caller.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        var payload = BuildExceptionPayload(
            errorCode: 60, name: "DB::TableNotFound",
            message: "table dribble missing", stackTrace: "s", hasNested: false);
        for (int i = 0; i < payload.Length; i++)
            mock.EnqueueBytes(payload.AsSpan(i, 1));
        mock.CompleteOutgoing();

        ClickHouseServerException? thrown = null;
        var pump = Task.Run(async () =>
        {
            try { await conn.ReceiveEndOfStreamAsync(CancellationToken.None); }
            catch (ClickHouseServerException ex) { thrown = ex; }
        });
        var winner = await Task.WhenAny(pump, Task.Delay(TimeSpan.FromSeconds(10)));
        Assert.Same(pump, winner);
        await pump;

        Assert.NotNull(thrown);
        Assert.Equal(60, thrown!.ErrorCode);
        Assert.Contains("table dribble missing", thrown.Message);
    }

    [Fact]
    public async Task ProgressInterleavedWithEos_PumpCompletes()
    {
        // Real CH sends Progress messages alongside the INSERT response. Pin that
        // the L3 Progress scan path consumes the right number of bytes — a regression
        // here (e.g. a new Progress field added without updating TryScan) would
        // cause the next message dispatch to parse garbage.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        var revision = MockClickHouseServer.PinnedProtocolRevision;
        var payload = BuildProgressPlusEos(revision);
        mock.EnqueueBytes(payload);

        var pump = conn.ReceiveEndOfStreamAsync(CancellationToken.None);
        var winner = await Task.WhenAny(pump, Task.Delay(AntiHangTimeout));
        Assert.Same(pump, winner);
        await pump;
        Assert.True(conn.CanBePooled);
    }

    [Fact]
    public async Task TrailingBytesAfterEos_ThrowsProtocolExceptionAndPoisons()
    {
        // L4 extension to the bulk-insert pump. Same-buffer trailing bytes after
        // EOS must surface as a typed protocol exception with the connection
        // poisoned — otherwise stale bytes would corrupt whatever query came next.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        // EOS byte + 16 bytes of junk in the same TCP packet.
        var payload = new byte[1 + 16];
        payload[0] = (byte)Protocol.ServerMessageType.EndOfStream;
        new Random(0xBEEF).NextBytes(payload.AsSpan(1));
        mock.EnqueueBytes(payload);

        var pump = Task.Run(() => conn.ReceiveEndOfStreamAsync(CancellationToken.None));
        var winner = await Task.WhenAny(pump, Task.Delay(AntiHangTimeout));
        Assert.Same(pump, winner);

        var ex = await Assert.ThrowsAsync<ClickHouseProtocolException>(() => pump);
        Assert.Contains("after EndOfStream during INSERT", ex.Message);
        Assert.False(conn.CanBePooled);
    }

    [Fact]
    public async Task Cancellation_RespectedDuringPartialRead()
    {
        // The pump must honour cancellation even mid-read. Send half an Exception
        // message (so the pump is waiting for more bytes) and never complete it.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        // Just the Exception type byte + 2 of 4 code bytes.
        mock.EnqueueBytes(new byte[] { (byte)Protocol.ServerMessageType.Exception, 0x01, 0x02 });

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        OperationCanceledException? caught = null;
        try { await conn.ReceiveEndOfStreamAsync(cts.Token); }
        catch (OperationCanceledException ex) { caught = ex; }

        Assert.NotNull(caught);
    }

    [Fact]
    public async Task TableColumnsThenEos_PumpCompletes()
    {
        // The pump skips TableColumns messages between Data and EOS. Pre-fix this
        // used throwing ReadString — partial TableColumns hit the catch-IOException
        // backstop. Post-fix uses TrySkipString explicitly. Drive a TableColumns
        // followed by EOS; the pump must complete cleanly.
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.TableColumns);
        w.WriteString("ext_table");
        w.WriteString("id UInt64, name String");
        w.WriteVarInt((ulong)Protocol.ServerMessageType.EndOfStream);

        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(bw.WrittenMemory.ToArray());

        var pump = conn.ReceiveEndOfStreamAsync(CancellationToken.None);
        var winner = await Task.WhenAny(pump, Task.Delay(AntiHangTimeout));
        Assert.Same(pump, winner);
        await pump;
        Assert.True(conn.CanBePooled);
    }

    [Fact]
    public async Task RealisticInsertResponse_DataProgressDataProfileInfoEos_PumpCompletes()
    {
        // Mirrors what a real ClickHouse server sends after an INSERT: an empty
        // confirmation Data block, a Progress message, an empty Data block (acks),
        // a ProfileInfo summary, and EOS. A regression in any single dispatch
        // path would cascade — the Progress consumes the wrong number of bytes
        // and the next Data parse goes off the rails. This test pins the
        // end-to-end sequence.
        var revision = MockClickHouseServer.PinnedProtocolRevision;
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);

        // Data #1
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Data);
        w.WriteString(string.Empty);
        Data.BlockInfo.Default.Write(ref w);
        w.WriteVarInt(0);
        w.WriteVarInt(0);

        // Progress
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Progress);
        w.WriteVarInt(100);
        w.WriteVarInt(2048);
        w.WriteVarInt(1000);
        if (revision >= ProtocolVersion.WithTotalBytesInProgress) w.WriteVarInt(20480);
        if (revision >= ProtocolVersion.WithClientWriteInfo) { w.WriteVarInt(0); w.WriteVarInt(0); }
        if (revision >= ProtocolVersion.WithServerQueryTimeInProgress) w.WriteVarInt(123456);

        // Data #2
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Data);
        w.WriteString(string.Empty);
        Data.BlockInfo.Default.Write(ref w);
        w.WriteVarInt(0);
        w.WriteVarInt(0);

        // ProfileInfo
        w.WriteVarInt((ulong)Protocol.ServerMessageType.ProfileInfo);
        WriteProfileInfoBody(ref w, revision);

        // EOS
        w.WriteVarInt((ulong)Protocol.ServerMessageType.EndOfStream);

        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(bw.WrittenMemory.ToArray());

        var pump = conn.ReceiveEndOfStreamAsync(CancellationToken.None);
        var winner = await Task.WhenAny(pump, Task.Delay(AntiHangTimeout));
        Assert.Same(pump, winner);
        await pump;
        Assert.True(conn.CanBePooled);
    }

    [Fact]
    public async Task RealisticInsertResponse_DribbledByteByByte_PumpCompletes()
    {
        // Same realistic sequence as above, but transmitted one byte at a time.
        // Catches any boundary-crossing bug in the multi-message dispatch — e.g.
        // a previous-message's body being misattributed to the next dispatch
        // because the state machine got confused at a fragmentation boundary.
        var revision = MockClickHouseServer.PinnedProtocolRevision;
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Data);
        w.WriteString(string.Empty);
        Data.BlockInfo.Default.Write(ref w);
        w.WriteVarInt(0);
        w.WriteVarInt(0);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Progress);
        w.WriteVarInt(100);
        w.WriteVarInt(2048);
        w.WriteVarInt(1000);
        if (revision >= ProtocolVersion.WithTotalBytesInProgress) w.WriteVarInt(20480);
        if (revision >= ProtocolVersion.WithClientWriteInfo) { w.WriteVarInt(0); w.WriteVarInt(0); }
        if (revision >= ProtocolVersion.WithServerQueryTimeInProgress) w.WriteVarInt(123456);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.ProfileInfo);
        WriteProfileInfoBody(ref w, revision);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.EndOfStream);
        var payload = bw.WrittenMemory.ToArray();

        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        for (int i = 0; i < payload.Length; i++)
            mock.EnqueueBytes(payload.AsSpan(i, 1));
        mock.CompleteOutgoing();

        var pump = conn.ReceiveEndOfStreamAsync(CancellationToken.None);
        var winner = await Task.WhenAny(pump, Task.Delay(TimeSpan.FromSeconds(15)));
        Assert.Same(pump, winner);
        await pump;
        Assert.True(conn.CanBePooled);
    }

    [Fact]
    public async Task RealisticInsertResponse_ExceptionAfterProgress_TypedThrow_ConnectionUsable()
    {
        // Server starts processing (sends a Data + Progress), then fails — sends
        // an Exception. The pump must surface the typed exception cleanly and
        // not poison the connection (Progress before Exception was structurally
        // valid; the bytes are fine, the query is what failed).
        var revision = MockClickHouseServer.PinnedProtocolRevision;
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);

        w.WriteVarInt((ulong)Protocol.ServerMessageType.Data);
        w.WriteString(string.Empty);
        Data.BlockInfo.Default.Write(ref w);
        w.WriteVarInt(0);
        w.WriteVarInt(0);

        w.WriteVarInt((ulong)Protocol.ServerMessageType.Progress);
        w.WriteVarInt(50);
        w.WriteVarInt(1024);
        w.WriteVarInt(1000);
        if (revision >= ProtocolVersion.WithTotalBytesInProgress) w.WriteVarInt(20480);
        if (revision >= ProtocolVersion.WithClientWriteInfo) { w.WriteVarInt(0); w.WriteVarInt(0); }
        if (revision >= ProtocolVersion.WithServerQueryTimeInProgress) w.WriteVarInt(123456);

        w.WriteVarInt((ulong)Protocol.ServerMessageType.Exception);
        w.WriteInt32(252);
        w.WriteString("DB::Exception");
        w.WriteString("INSERT failed mid-stream");
        w.WriteString("stack");
        w.WriteByte(0);

        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(bw.WrittenMemory.ToArray());

        ClickHouseServerException? thrown = null;
        try { await conn.ReceiveEndOfStreamAsync(CancellationToken.None); }
        catch (ClickHouseServerException ex) { thrown = ex; }

        Assert.NotNull(thrown);
        Assert.Equal(252, thrown!.ErrorCode);
        Assert.Contains("INSERT failed mid-stream", thrown.Message);
        Assert.True(conn.CanBePooled, "well-formed Exception in INSERT response must not poison");
    }

    [Fact]
    public async Task ProfileInfoThenEos_PumpCompletes()
    {
        // ProfileInfo dispatch in the bulk-insert pump uses TrySkipProfileInfo
        // (added as part of #3 hardening). A complete ProfileInfo followed by EOS
        // must be skipped cleanly; a regression here would either deadlock or
        // throw a wrapped IOException.
        var revision = MockClickHouseServer.PinnedProtocolRevision;
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.ProfileInfo);
        WriteProfileInfoBody(ref w, revision);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.EndOfStream);

        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(bw.WrittenMemory.ToArray());

        var pump = conn.ReceiveEndOfStreamAsync(CancellationToken.None);
        var winner = await Task.WhenAny(pump, Task.Delay(AntiHangTimeout));
        Assert.Same(pump, winner);
        await pump;
        Assert.True(conn.CanBePooled);
    }

    [Fact]
    public async Task ProfileInfoDribbledByteByByte_PumpCompletes()
    {
        // The L3 hardening for ProfileInfo (TrySkipProfileInfo + scan-then-skip)
        // means a partial ProfileInfo returns "wait for more" without going through
        // the catch-IOException backstop. Drive a ProfileInfo + EOS one byte at
        // a time and assert no false-positive failure.
        var revision = MockClickHouseServer.PinnedProtocolRevision;
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.ProfileInfo);
        WriteProfileInfoBody(ref w, revision);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.EndOfStream);
        var payload = bw.WrittenMemory.ToArray();

        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        for (int i = 0; i < payload.Length; i++)
            mock.EnqueueBytes(payload.AsSpan(i, 1));
        mock.CompleteOutgoing();

        var pump = conn.ReceiveEndOfStreamAsync(CancellationToken.None);
        var winner = await Task.WhenAny(pump, Task.Delay(TimeSpan.FromSeconds(10)));
        Assert.Same(pump, winner);
        await pump;
        Assert.True(conn.CanBePooled);
    }

    private static void WriteProfileInfoBody(ref ProtocolWriter w, int revision)
    {
        w.WriteVarInt(100);  // rows
        w.WriteVarInt(2);    // blocks
        w.WriteVarInt(2048); // bytes
        w.WriteByte(0);      // applied_limit
        w.WriteVarInt(0);    // rows_before_limit
        w.WriteByte(0);      // calculated_rows_before_limit
        if (revision >= ProtocolVersion.WithRowsBeforeAggregation)
        {
            w.WriteByte(0);     // applied_aggregation
            w.WriteVarInt(0);   // rows_before_aggregation
        }
    }

    private static byte[] BuildExceptionPayload(int errorCode, string name, string message, string stackTrace, bool hasNested)
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

    private static byte[] BuildProgressPlusEos(int revision)
    {
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Progress);
        w.WriteVarInt(100);   // rows
        w.WriteVarInt(2048);  // bytes
        w.WriteVarInt(1000);  // totalRows
        if (revision >= ProtocolVersion.WithTotalBytesInProgress) w.WriteVarInt(20480);
        if (revision >= ProtocolVersion.WithClientWriteInfo) { w.WriteVarInt(0); w.WriteVarInt(0); }
        if (revision >= ProtocolVersion.WithServerQueryTimeInProgress) w.WriteVarInt(123456);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.EndOfStream);
        return bw.WrittenMemory.ToArray();
    }
}
