using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// Pins the wire-evidence half of the conversation state machine — the part the
/// pure-bookkeeping unit tests can't reach because it needs a real open socket:
/// <c>WireIndeterminate</c>'s inputs, and the pessimistic conviction in
/// <c>ExitBusyResolve</c> that consumes it.
/// </summary>
/// <remarks>
/// <para>The predicate is <c>_isOpen &amp;&amp; _conversationWrote &amp;&amp;
/// !_boundaryProven &amp;&amp; !_protocolFatal</c>, and both directions matter:</para>
/// <list type="bullet">
/// <item><description><b>Convict</b> when bytes went out and no response terminator
/// came back. The wire offset is unknown, so the next query on this connection would
/// read the abandoned response as its own — the failure this whole mechanism exists
/// to prevent.</description></item>
/// <item><description><b>Don't convict</b> when a terminator WAS consumed. A server
/// exception ends the response with the server back at idle; poisoning there burns a
/// healthy socket on every error the caller deliberately provoked.</description></item>
/// </list>
/// <para>The mock server is what makes the first case testable at all: a real
/// ClickHouse always answers, so "bytes out, nothing back" needs a server that can be
/// told to stay silent.</para>
/// </remarks>
[Trait(Categories.Name, Categories.Streams)]
public class ConversationResolveEvidenceTests
{
    private static readonly TimeSpan AntiHangTimeout = TimeSpan.FromSeconds(5);

    private static async Task<(ClickHouseConnection conn, MockClickHouseServer.Session session)>
        ConnectAsync(MockClickHouseServer mock)
    {
        var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        var session = await mock.AcceptNextSessionAsync().AsTask().WaitAsync(AntiHangTimeout);
        await session.HandshakeCompleted.WaitAsync(AntiHangTimeout);
        return (conn, session);
    }

    /// <summary>
    /// Blocks until the QUERY's bytes have reached the wire and no terminator has
    /// come back — i.e. the connection is genuinely mid-conversation.
    /// </summary>
    /// <remarks>
    /// The mock never reads client traffic, so there is no server-side "query
    /// received" signal to wait on. Polling <c>ConversationWrote</c> alone is a trap:
    /// the handshake is itself a write, so that flag is still true from OpenAsync
    /// until the query's <c>EnterBusy</c> resets it — a poll on it returns
    /// immediately and the test then cancels a conversation that never started.
    /// <c>WireIndeterminate</c> is the conjunction that can only hold once the
    /// query has both claimed the slot (clearing the handshake's boundary proof)
    /// and put its own bytes out.
    /// </remarks>
    private static async Task WaitForMidConversationAsync(ClickHouseConnection conn)
    {
        using var deadline = new CancellationTokenSource(AntiHangTimeout);
        while (!conn.WireIndeterminate)
        {
            deadline.Token.ThrowIfCancellationRequested();
            await Task.Delay(5, deadline.Token);
        }
    }

    [Fact]
    public async Task FreshlyOpenedConnection_HasProvenBoundary()
    {
        // Baseline: a completed handshake is a proof site. Without this the very
        // first ExitBusyResolve on every connection would convict.
        await using var mock = new MockClickHouseServer();
        mock.Start();
        var (conn, _) = await ConnectAsync(mock);
        await using var _c = conn;

        Assert.False(conn.WireIndeterminate);
        Assert.True(conn.CanBePooled);
    }

    [Fact]
    public async Task QueryWithNoResponse_LeavesWireIndeterminate_AndResolveConvicts()
    {
        // Bytes out, server silent, no terminator consumed → the wire offset is
        // unknown. Cancelling the read must leave the connection convicted so the
        // pool discards it rather than handing the abandoned response to the next
        // caller as its own result.
        //
        // The socket is killed after the cancellation so the post-cancel drain
        // reaches its "server closed" exit immediately instead of sitting out the
        // 30s drain cap. That exit is deliberately proof-free (see the NOTE in
        // DrainAfterCancellationAsync), so the conviction being asserted here is
        // the same one a silent-server timeout produces — just without paying 30s
        // of wall clock for it in every CI run.
        await using var mock = new MockClickHouseServer();
        mock.Start();
        var (conn, session) = await ConnectAsync(mock);
        await using var _c = conn;

        using var cts = new CancellationTokenSource();
        var query = Task.Run(async () =>
        {
            await foreach (var _ in conn.QueryStreamAsync<int>("SELECT 1", cancellationToken: cts.Token)) { }
        });

        await WaitForMidConversationAsync(conn);
        cts.Cancel();
        session.Dispose();

        await Assert.ThrowsAnyAsync<Exception>(() => query);

        Assert.False(conn.CanBePooled,
            "a conversation that wrote and never saw a terminator must not look reusable");
    }

    [Fact]
    public async Task ServerExceptionResponse_ProvesBoundary_AndResolveDoesNotConvict()
    {
        // The other direction. A well-formed exception envelope IS a terminator:
        // the server consumed the query and is back at idle. Convicting here (or
        // running the reader's fallback drain, which would read against a silent
        // server until the 30s cap) would burn a healthy connection.
        await using var mock = new MockClickHouseServer();
        mock.Start();
        var (conn, session) = await ConnectAsync(mock);
        await using var _c = conn;

        session.EnqueueBytes(BuildException(errorCode: 62, message: "Syntax error"));

        await Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            await foreach (var _ in conn.QueryStreamAsync<int>("SELECT bad")) { }
        });

        Assert.False(conn.WireIndeterminate, "a consumed exception envelope proves the boundary");
        Assert.True(conn.CanBePooled, "a server-reported error must not poison the connection");
    }

    [Fact]
    public async Task CleanEndOfStream_ProvesBoundary_AndConnectionStaysUsable()
    {
        await using var mock = new MockClickHouseServer();
        mock.Start();
        var (conn, session) = await ConnectAsync(mock);
        await using var _c = conn;

        session.EnqueueBytes(BuildEmptyBlockPlusEos());
        await foreach (var _ in conn.QueryStreamAsync<int>("SELECT 1")) { }

        Assert.False(conn.WireIndeterminate);
        Assert.True(conn.CanBePooled);
    }

    [Fact]
    public async Task AbandonedReaderDispose_RunsFallbackDrain_WhenWireIsIndeterminate()
    {
        // The reader's dispose safety-net. The pump enumerator is finished after the
        // cancellation, so its own drain loop reads nothing; the connection-level
        // fallback drain is what actually realigns the wire. Gated on the same
        // evidence predicate, so this asserts the gate lets the drain run (and the
        // drain, finding a silent server, convicts) rather than silently skipping it
        // and leaving the response for the next query.
        await using var mock = new MockClickHouseServer();
        mock.Start();
        var (conn, session) = await ConnectAsync(mock);
        await using var _c = conn;

        using var cts = new CancellationTokenSource();
        var reader = await conn.ExecuteReaderAsync("SELECT 1", cancellationToken: cts.Token);

        var read = Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            while (await reader.ReadAsync(cts.Token)) { }
        });

        await WaitForMidConversationAsync(conn);
        cts.Cancel();
        await read;

        // Socket killed before dispose so the fallback drain hits its "server
        // closed" exit rather than sitting out the 30s cap. That exit is
        // proof-free by design, so the gate below still sees an indeterminate
        // wire — the branch under test — without the wall-clock cost.
        session.Dispose();
        await reader.DisposeAsync().AsTask().WaitAsync(AntiHangTimeout);

        Assert.False(conn.CanBePooled,
            "dispose over an indeterminate wire must leave the connection unpoolable");
    }

    [Fact]
    public async Task ReaderDisposedAfterCleanEos_DoesNotConvict()
    {
        // Negative control for the gate above: a reader that reached EOS has a
        // proven boundary, so dispose must skip the fallback drain entirely. If the
        // gate regressed to "always drain", this test would block for the 30s drain
        // cap against the silent mock and then fail on CanBePooled.
        await using var mock = new MockClickHouseServer();
        mock.Start();
        var (conn, session) = await ConnectAsync(mock);
        await using var _c = conn;

        session.EnqueueBytes(BuildEmptyBlockPlusEos());

        var reader = await conn.ExecuteReaderAsync("SELECT 1");
        while (await reader.ReadAsync()) { }
        await reader.DisposeAsync().AsTask().WaitAsync(AntiHangTimeout);

        Assert.True(conn.CanBePooled);
    }

    [Fact]
    public async Task SuccessorConversation_SurvivesStaleReaderDispose()
    {
        // End-to-end version of the epoch gate: the reader releases its slot at EOS
        // so a successor query can start, then the reader is disposed carrying the
        // OLD conversation's identity. The successor must be untouched — same
        // caller-supplied query id on both, so only the epoch distinguishes them.
        await using var mock = new MockClickHouseServer();
        mock.Start();
        var (conn, session) = await ConnectAsync(mock);
        await using var _c = conn;

        session.EnqueueBytes(BuildEmptyBlockPlusEos());
        var reader = await conn.ExecuteReaderAsync("SELECT 1", queryId: "reused-id");
        while (await reader.ReadAsync()) { }

        // Successor claims the slot with the SAME id before the reader is disposed.
        session.EnqueueBytes(BuildEmptyBlockPlusEos());
        var successor = await conn.ExecuteReaderAsync("SELECT 2", queryId: "reused-id");

        // Stale dispose fires here — must no-op against the successor.
        await reader.DisposeAsync().AsTask().WaitAsync(AntiHangTimeout);

        while (await successor.ReadAsync()) { }
        await successor.DisposeAsync().AsTask().WaitAsync(AntiHangTimeout);

        Assert.True(conn.CanBePooled,
            "a stale reader dispose must not poison the successor's conversation");
    }

    // ---- scripted server payloads ------------------------------------------

    private static byte[] BuildEmptyBlockPlusEos()
    {
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)CH.Native.Protocol.ServerMessageType.Data);
        w.WriteString(string.Empty);
        CH.Native.Data.BlockInfo.Default.Write(ref w);
        w.WriteVarInt(0); // columns
        w.WriteVarInt(0); // rows
        w.WriteVarInt((ulong)CH.Native.Protocol.ServerMessageType.EndOfStream);
        return bw.WrittenMemory.ToArray();
    }

    private static byte[] BuildException(int errorCode, string message)
    {
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        // Layout — keep in sync with ExceptionMessage.Read:
        // Int32 code, String name, String message, String stackTrace, Byte hasNested.
        w.WriteVarInt((ulong)CH.Native.Protocol.ServerMessageType.Exception);
        w.WriteInt32(errorCode);
        w.WriteString("DB::Exception");
        w.WriteString(message);
        w.WriteString(string.Empty);
        w.WriteByte(0);
        return bw.WrittenMemory.ToArray();
    }
}
