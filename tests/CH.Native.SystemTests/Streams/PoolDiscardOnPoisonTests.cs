using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// End-to-end pin: when the bulk-insert pump (or main pump) sets
/// <c>_protocolFatal</c>, the next pool checkout must return a fresh connection,
/// not the poisoned one. The pump-level tests already verify
/// <c>conn.CanBePooled == false</c>; these tests verify the pool actually consults
/// that flag and discards on return.
///
/// <para>
/// Without this guarantee the bulk-insert hardening (L1/L3/L4 in
/// <c>ReceiveEndOfStreamAsync</c>) would set the poison flag but the pool would
/// happily reuse the corrupted-stream connection — a much subtler failure mode
/// than a deadlock.
/// </para>
/// </summary>
[Trait(Categories.Name, Categories.Streams)]
public class PoolDiscardOnPoisonTests
{
    private static readonly TimeSpan AntiHangTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task BulkInsertPump_PoisonsConnection_PoolDiscardsAndOpensFreshOne()
    {
        // Two-session test: the first connection is poisoned via the bulk-insert
        // pump (unknown message type triggers L1), then disposed (returns to pool
        // → pool sees !CanBePooled → discards). A second OpenConnectionAsync call
        // must accept a brand-new TCP session on the mock side.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        var options = new ClickHouseDataSourceOptions { Settings = mock.BuildSettings() };
        await using var dataSource = new ClickHouseDataSource(options);

        // --- conn 1 ---
        var conn1 = await dataSource.OpenConnectionAsync();
        var session1 = await mock.AcceptNextSessionAsync()
            .AsTask().WaitAsync(AntiHangTimeout);
        await session1.HandshakeCompleted.WaitAsync(AntiHangTimeout);

        // Drive the bulk-insert pump into L1 (unknown message type) — this sets
        // _protocolFatal so CanBePooled goes false.
        session1.EnqueueBytes(new byte[] { 0xFF, 0x01 });
        await Assert.ThrowsAsync<ClickHouseProtocolException>(
            () => conn1.ReceiveEndOfStreamAsync(CancellationToken.None));
        Assert.False(conn1.CanBePooled, "pump must have poisoned the connection");

        // Returning to the pool: the pool's ReturnAsync inspects CanBePooled and
        // routes to DiscardAsync. The mock-side socket must close as a result.
        // Returning to the pool: ReturnAsync inspects CanBePooled and routes to
        // DiscardAsync. We don't wait on the mock-side socket-closed signal —
        // the mock can't observe a client-side TCP shutdown without polling the
        // socket, and the contract we actually care about is below: the next
        // checkout opens a brand-new TCP session.
        await conn1.DisposeAsync();

        // --- conn 2 ---
        // If the pool reused the poisoned connection, no new TCP session would
        // appear on the mock; AcceptNextSessionAsync would time out.
        var conn2 = await dataSource.OpenConnectionAsync();
        var session2 = await mock.AcceptNextSessionAsync()
            .AsTask().WaitAsync(AntiHangTimeout);
        await session2.HandshakeCompleted.WaitAsync(AntiHangTimeout);

        Assert.NotSame(session1, session2);
        Assert.True(conn2.CanBePooled, "fresh connection must not inherit poison");
        await conn2.DisposeAsync();
    }

    [Fact]
    public async Task MainPump_PoisonsConnection_PoolDiscardsAndOpensFreshOne()
    {
        // Same end-to-end pin but driving the main pump (L4: extra bytes after EOS).
        // Verifies that a poison set by ANY pump path reaches the pool's discard.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        var options = new ClickHouseDataSourceOptions { Settings = mock.BuildSettings() };
        await using var dataSource = new ClickHouseDataSource(options);

        var conn1 = await dataSource.OpenConnectionAsync();
        var session1 = await mock.AcceptNextSessionAsync()
            .AsTask().WaitAsync(AntiHangTimeout);
        await session1.HandshakeCompleted.WaitAsync(AntiHangTimeout);

        // Empty Data + EOS + 16 junk bytes → L4 fires.
        session1.EnqueueBytes(BuildEmptyBlockPlusEosPlusJunk(junkBytes: 16));

        var query = Task.Run(async () =>
        {
            await foreach (var _ in conn1.QueryAsync<int>("SELECT 1")) { }
        });
        await Assert.ThrowsAsync<ClickHouseProtocolException>(() => query);
        Assert.False(conn1.CanBePooled);

        await conn1.DisposeAsync();

        var conn2 = await dataSource.OpenConnectionAsync();
        var session2 = await mock.AcceptNextSessionAsync()
            .AsTask().WaitAsync(AntiHangTimeout);
        await session2.HandshakeCompleted.WaitAsync(AntiHangTimeout);

        Assert.NotSame(session1, session2);
        await conn2.DisposeAsync();
    }

    [Fact]
    public async Task CleanConnection_PoolReusesWithoutNewTcpSession()
    {
        // Negative control: a CLEAN connection (no poison) returned to the pool
        // must NOT trigger a new TCP session on the next checkout — proves the
        // discard path above is actually doing something distinguishable.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        var options = new ClickHouseDataSourceOptions { Settings = mock.BuildSettings() };
        await using var dataSource = new ClickHouseDataSource(options);

        var conn1 = await dataSource.OpenConnectionAsync();
        var session1 = await mock.AcceptNextSessionAsync()
            .AsTask().WaitAsync(AntiHangTimeout);
        await session1.HandshakeCompleted.WaitAsync(AntiHangTimeout);

        // Drive a clean Data + EOS round to leave the connection in a usable state.
        session1.EnqueueBytes(BuildEmptyBlockPlusEosPlusJunk(junkBytes: 0));
        await foreach (var _ in conn1.QueryAsync<int>("SELECT 1")) { }
        Assert.True(conn1.CanBePooled);

        await conn1.DisposeAsync();
        // Do NOT await SocketClosed — a pooled connection's socket stays open.

        // Second checkout: must reuse the existing session (no new accept).
        var conn2 = await dataSource.OpenConnectionAsync();

        // Try to accept a new session with a short timeout — this should TIME OUT
        // because the pool reused conn1's TCP session.
        using var noNewCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await mock.AcceptNextSessionAsync(noNewCts.Token);
        });

        await conn2.DisposeAsync();
    }

    private static byte[] BuildEmptyBlockPlusEosPlusJunk(int junkBytes)
    {
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);

        // Data message: empty block.
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Data);
        w.WriteString(string.Empty);
        Data.BlockInfo.Default.Write(ref w);
        w.WriteVarInt(0);
        w.WriteVarInt(0);

        // EOS.
        w.WriteVarInt((ulong)Protocol.ServerMessageType.EndOfStream);

        if (junkBytes > 0)
        {
            var junk = new byte[junkBytes];
            new Random(0xC0FFEE).NextBytes(junk);
            w.WriteBytes(junk);
        }
        return bw.WrittenMemory.ToArray();
    }
}
