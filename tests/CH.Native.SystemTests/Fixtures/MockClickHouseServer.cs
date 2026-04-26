using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using CH.Native.Connection;
using CH.Native.Protocol;
using CH.Native.Protocol.Messages;

namespace CH.Native.SystemTests.Fixtures;

/// <summary>
/// Minimal ClickHouse server impersonator for protocol-edge tests. Performs the
/// real ClientHello/ServerHello handshake against a configurable protocol revision,
/// then writes whatever scripted bytes the test pushes via <see cref="EnqueueBytes"/>.
///
/// <para>
/// The point is to drive the post-handshake parsing path with arbitrary bytes that
/// a real ClickHouse server would never send (unknown message types, truncated
/// frames, byte-dribbles, oversized strings). Toxiproxy can shape the network but
/// can't produce precisely-shaped garbage; this fixture can.
/// </para>
///
/// <para><b>Sessions.</b> Each accepted client connection becomes a <see cref="Session"/>
/// with its own scripted byte queue and handshake-completion task. Tests that exercise
/// connection reuse / pooling can call <see cref="AcceptNextSessionAsync"/> to wait
/// for the next client. Single-session tests can use the convenience properties
/// (<see cref="HandshakeCompleted"/>, <see cref="EnqueueBytes"/>, etc.) which target
/// the first session — implicitly created when the first client connects.</para>
///
/// <para>Designed to be reused for Gap 3 and Gap 6 (and any future protocol-edge
/// work). Deliberately dumb: handshake plus a queue of byte payloads — anything
/// richer competes with using a real ClickHouse instance.</para>
/// </summary>
public sealed class MockClickHouseServer : IAsyncDisposable
{
    // Pin a known, modern revision so the wire shape stays deterministic across
    // test runs. Tests script bytes against exactly this revision — bumping this
    // requires re-validating every scripted payload.
    public const int PinnedProtocolRevision = ProtocolVersion.Current;

    private readonly TcpListener _listener;
    private readonly Channel<Session> _newSessions;
    private readonly CancellationTokenSource _cts = new();
    private readonly List<Session> _allSessions = new();
    private readonly object _sessionsLock = new();
    private Task? _acceptLoop;
    private Session? _firstSessionCache;

    public MockClickHouseServer()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _newSessions = Channel.CreateUnbounded<Session>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = true,
        });
    }

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    /// <summary>
    /// Convenience: handshake-completed task for the first session. Awaiting this
    /// blocks until a client has connected and completed the handshake. Multi-session
    /// tests should use <see cref="AcceptNextSessionAsync"/> directly.
    /// </summary>
    public Task HandshakeCompleted => GetOrAwaitFirstSession().Task
        .ContinueWith(t => t.Result.HandshakeCompleted, TaskScheduler.Default).Unwrap();

    /// <summary>
    /// Snapshot of all sessions accepted so far (in connection order). Useful for
    /// asserting "did the pool open a second connection".
    /// </summary>
    public IReadOnlyList<Session> Sessions
    {
        get { lock (_sessionsLock) return _allSessions.ToArray(); }
    }

    public void Start()
    {
        _listener.Start();
        _acceptLoop = Task.Run(() => AcceptLoopAsync(_cts.Token));
    }

    public ClickHouseConnectionSettings BuildSettings(
        Action<ClickHouseConnectionSettingsBuilder>? configure = null)
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("127.0.0.1")
            .WithPort(Port)
            .WithCredentials("default", string.Empty)
            .WithCompression(false);
        configure?.Invoke(builder);
        return builder.Build();
    }

    /// <summary>
    /// Convenience for single-session tests: enqueue bytes on the first session
    /// (waits for the first client to connect if it hasn't already).
    /// </summary>
    public void EnqueueBytes(ReadOnlySpan<byte> bytes)
    {
        var session = WaitForFirstSession();
        session.EnqueueBytes(bytes);
    }

    /// <summary>
    /// Convenience for single-session tests: complete the first session's outgoing
    /// queue.
    /// </summary>
    public void CompleteOutgoing() => WaitForFirstSession().CompleteOutgoing();

    /// <summary>
    /// Wait for the next client connection (after any already-accepted ones) and
    /// return its <see cref="Session"/>. Use for tests that exercise reconnection
    /// or pooling — each call returns a distinct session, in connection order.
    /// </summary>
    public ValueTask<Session> AcceptNextSessionAsync(CancellationToken ct = default)
        => _newSessions.Reader.ReadAsync(ct);

    private TaskCompletionSource<Session> GetOrAwaitFirstSession()
    {
        lock (_sessionsLock)
        {
            var tcs = new TaskCompletionSource<Session>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (_firstSessionCache is not null)
            {
                tcs.SetResult(_firstSessionCache);
                return tcs;
            }

            // Spawn a task that awaits the channel — keeps this method synchronous.
            _ = Task.Run(async () =>
            {
                try
                {
                    var session = await _newSessions.Reader.ReadAsync(_cts.Token);
                    tcs.TrySetResult(session);
                }
                catch (Exception ex) { tcs.TrySetException(ex); }
            });
            return tcs;
        }
    }

    private Session WaitForFirstSession()
    {
        if (_firstSessionCache is not null) return _firstSessionCache;
        var tcs = GetOrAwaitFirstSession();
        var session = tcs.Task.GetAwaiter().GetResult();
        _firstSessionCache = session;
        return session;
    }

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(ct);
                var session = new Session(client);

                lock (_sessionsLock)
                {
                    _allSessions.Add(session);
                    if (_firstSessionCache is null)
                        _firstSessionCache = session;
                }

                _newSessions.Writer.TryWrite(session);

                // Each session runs in its own task so accept-loop stays responsive.
                _ = Task.Run(() => session.RunAsync(ct));
            }
        }
        catch (OperationCanceledException) { /* expected during dispose */ }
        catch (Exception)
        {
            // Listener torn down or socket error — propagate via session-handshake
            // task so the next AcceptNextSessionAsync surfaces it.
            _newSessions.Writer.TryComplete();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { _listener.Stop(); } catch { /* already stopped */ }
        try { _newSessions.Writer.TryComplete(); } catch { /* already done */ }

        Session[] sessions;
        lock (_sessionsLock) { sessions = _allSessions.ToArray(); }
        foreach (var session in sessions)
            session.Dispose();

        if (_acceptLoop is not null)
        {
            try { await _acceptLoop.WaitAsync(TimeSpan.FromSeconds(2)); }
            catch { /* swallow — fixture is shutting down */ }
        }
        _cts.Dispose();
    }

    /// <summary>
    /// Single accepted-client session. Owns its TCP socket, an outgoing scripted-byte
    /// queue, and a handshake-completion task.
    /// </summary>
    public sealed class Session : IDisposable
    {
        private readonly TcpClient _client;
        private readonly Channel<byte[]> _outgoing;
        private readonly TaskCompletionSource<bool> _handshakeComplete =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _socketClosed =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Session(TcpClient client)
        {
            _client = client;
            _outgoing = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false,
            });
        }

        public Task HandshakeCompleted => _handshakeComplete.Task;

        /// <summary>Completes once the mock-side socket has been closed (server
        /// finished writing and called Shutdown). Useful for asserting that the
        /// pool actually discarded the connection.</summary>
        public Task SocketClosed => _socketClosed.Task;

        public void EnqueueBytes(ReadOnlySpan<byte> bytes)
            => _outgoing.Writer.TryWrite(bytes.ToArray());

        public void CompleteOutgoing() => _outgoing.Writer.TryComplete();

        internal async Task RunAsync(CancellationToken ct)
        {
            try
            {
                using var stream = _client.GetStream();

                await PerformHandshakeAsync(stream, ct);
                _handshakeComplete.TrySetResult(true);

                await foreach (var chunk in _outgoing.Reader.ReadAllAsync(ct))
                {
                    await stream.WriteAsync(chunk, ct);
                    await stream.FlushAsync(ct);
                }

                try { _client.Client.Shutdown(SocketShutdown.Send); }
                catch { /* socket may already be torn down by dispose */ }
            }
            catch (OperationCanceledException) { /* expected on dispose */ }
            catch (Exception ex)
            {
                _handshakeComplete.TrySetException(ex);
            }
            finally
            {
                _socketClosed.TrySetResult(true);
            }
        }

        public void Dispose()
        {
            try { _outgoing.Writer.TryComplete(); } catch { }
            try { _client.Dispose(); } catch { }
        }
    }

    private static async Task PerformHandshakeAsync(NetworkStream stream, CancellationToken ct)
    {
        // Read ClientHello off the wire by parsing it incrementally. ClientHello
        // is: varint(type=0), string(name), varint(major), varint(minor),
        // varint(revision), string(database), string(user), string(password).
        // We don't actually need the values — we just need to consume the bytes
        // before sending ServerHello, otherwise the client may receive ServerHello
        // before its own write completes (real CH does the same).
        var reader = new IncrementalReader(stream);
        await reader.ReadVarIntAsync(ct);   // message type
        await reader.ReadStringAsync(ct);   // client name
        await reader.ReadVarIntAsync(ct);   // major
        await reader.ReadVarIntAsync(ct);   // minor
        var clientRevision = await reader.ReadVarIntAsync(ct);
        await reader.ReadStringAsync(ct);   // database
        await reader.ReadStringAsync(ct);   // username
        await reader.ReadStringAsync(ct);   // password

        var negotiated = (int)Math.Min((ulong)PinnedProtocolRevision, clientRevision);

        // ProtocolWriter is a ref struct so it can't live across an await — do
        // the wire composition in a synchronous helper that returns a heap array.
        var helloBytes = ComposeServerHello(negotiated);
        await stream.WriteAsync(helloBytes, ct);
        await stream.FlushAsync(ct);

        // Read addendum if the negotiated revision uses it. Order mirrors
        // SendHelloAddendumAsync in ClickHouseConnection.
        if (negotiated >= ProtocolVersion.WithAddendum)
        {
            await reader.ReadStringAsync(ct); // quota_key

            if (negotiated >= ProtocolVersion.WithChunkedPackets)
            {
                await reader.ReadStringAsync(ct); // proto_send_chunked
                await reader.ReadStringAsync(ct); // proto_recv_chunked
            }

            if (negotiated >= ProtocolVersion.WithVersionedParallelReplicas)
            {
                await reader.ReadVarIntAsync(ct); // parallel-replicas version
            }
        }
    }

    private static byte[] ComposeServerHello(int negotiated)
    {
        var bufferWriter = new ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bufferWriter);
        w.WriteVarInt((ulong)ServerMessageType.Hello);
        w.WriteString("MockClickHouse");
        w.WriteVarInt(24);   // version major
        w.WriteVarInt(1);    // version minor
        w.WriteVarInt((ulong)negotiated);

        if (negotiated >= ProtocolVersion.WithVersionedParallelReplicas)
            w.WriteVarInt(0);

        if (negotiated >= ProtocolVersion.WithTimezone)
        {
            w.WriteString("UTC");
            w.WriteString("mock");
        }

        if (negotiated >= ProtocolVersion.WithVersionPatch)
            w.WriteVarInt(0);

        if (negotiated >= ProtocolVersion.WithChunkedPackets)
        {
            w.WriteString("notchunked");
            w.WriteString("notchunked");
        }

        if (negotiated >= ProtocolVersion.WithPasswordComplexityRules)
            w.WriteVarInt(0);

        if (negotiated >= ProtocolVersion.WithInterServerSecretV2)
            w.WriteUInt64(0);

        return bufferWriter.WrittenMemory.ToArray();
    }

    /// <summary>
    /// Tiny stream-side reader that handles the handshake without dragging in
    /// ProtocolReader (which expects a ReadOnlySequence, not a NetworkStream).
    /// </summary>
    private sealed class IncrementalReader
    {
        private readonly NetworkStream _stream;
        private readonly byte[] _scratch = new byte[1];

        public IncrementalReader(NetworkStream stream) => _stream = stream;

        public async Task<ulong> ReadVarIntAsync(CancellationToken ct)
        {
            ulong value = 0;
            int shift = 0;
            for (int i = 0; i < VarInt.MaxLength; i++)
            {
                int n = await _stream.ReadAsync(_scratch.AsMemory(0, 1), ct);
                if (n == 0)
                    throw new EndOfStreamException("Client closed during VarInt read");
                byte b = _scratch[0];
                value |= (ulong)(b & 0x7F) << shift;
                if ((b & 0x80) == 0)
                    return value;
                shift += 7;
            }
            throw new InvalidDataException("Malformed VarInt from client");
        }

        public async Task<string> ReadStringAsync(CancellationToken ct)
        {
            var len = (int)await ReadVarIntAsync(ct);
            if (len == 0) return string.Empty;
            var buf = new byte[len];
            int off = 0;
            while (off < len)
            {
                int n = await _stream.ReadAsync(buf.AsMemory(off, len - off), ct);
                if (n == 0)
                    throw new EndOfStreamException("Client closed during String read");
                off += n;
            }
            return System.Text.Encoding.UTF8.GetString(buf);
        }
    }
}
