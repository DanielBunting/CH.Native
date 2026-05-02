using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Protocol;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// Mock-server probes for the §7 reader contract: what does the library do when the
/// server's declared row count doesn't match the bytes that follow? Two shapes:
/// declared-greater (server promises more rows than it sends, then closes) and
/// declared-less (server sends extra row bytes after declared count). Both are
/// undefined behaviour territory — this fixture pins what the library does today.
/// </summary>
[Trait(Categories.Name, Categories.Streams)]
public class RowCountMismatchProbeTests
{
    private static readonly TimeSpan AntiHangTimeout = TimeSpan.FromSeconds(8);
    private readonly ITestOutputHelper _output;

    public RowCountMismatchProbeTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task ServerSendsLess_DeclaredHundred_OnlyFifty_ThrowsOrCancelsCleanly()
    {
        // num_rows=100 declared, only 50 UInt64 values (= 400 bytes) provided, then
        // the connection drops. The reader should surface a typed exception or honour
        // cancellation — never deadlock waiting for never-arriving bytes.
        var framed = ComposeUInt64Block(declaredRowCount: 100, providedRowCount: 50);

        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(framed);
        // Don't close stream — the reader is blocked waiting for more bytes that
        // never come. Cancellation is the safety net.

        using var cts = new CancellationTokenSource(AntiHangTimeout);
        Exception? caught = null;
        try
        {
            await foreach (var _ in conn.QueryAsync<ulong>("SELECT n FROM mock")
                .WithCancellation(cts.Token)) { }
        }
        catch (Exception ex) { caught = ex; }

        _output.WriteLine($"Server-sends-less surface: {caught?.GetType().FullName} — {caught?.Message}");
        Assert.NotNull(caught);
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    [Fact]
    public async Task ServerSendsLess_DeclaredHundred_ZeroBytes_ThrowsOrCancelsCleanly()
    {
        // Even more extreme: declare 100 rows, send 0 bytes of column data.
        var framed = ComposeUInt64Block(declaredRowCount: 100, providedRowCount: 0);

        await using var mock = new MockClickHouseServer();
        mock.Start();
        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;
        mock.EnqueueBytes(framed);

        using var cts = new CancellationTokenSource(AntiHangTimeout);
        Exception? caught = null;
        try
        {
            await foreach (var _ in conn.QueryAsync<ulong>("SELECT n FROM mock")
                .WithCancellation(cts.Token)) { }
        }
        catch (Exception ex) { caught = ex; }

        _output.WriteLine($"Server-sends-zero surface: {caught?.GetType().FullName}");
        Assert.NotNull(caught);
        Assert.IsNotType<OutOfMemoryException>(caught);
    }

    [Fact]
    public async Task ServerSendsMore_DeclaredHundred_OneFifty_TrailingBytesPoisonOrIgnored()
    {
        // num_rows=100 declared, 150 values provided. The reader consumes 100, then
        // either (a) continues reading and trips L4's "after EndOfStream" defence,
        // (b) reads the trailing bytes as a new message and fails L1, or
        // (c) silently ignores the extra. Probe — any of (a)/(b) is acceptable;
        // (c) would be a bug.
        var framed = ComposeUInt64Block(declaredRowCount: 100, providedRowCount: 150);

        // Append EndOfStream so the dispatch loop has somewhere to land if (a) fires.
        var bw = new ArrayBufferWriter<byte>();
        bw.Write(framed);
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)ServerMessageType.EndOfStream);
        var withEos = bw.WrittenMemory.ToArray();

        await using var mock = new MockClickHouseServer();
        mock.Start();
        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;
        mock.EnqueueBytes(withEos);
        mock.CompleteOutgoing();

        int rowsObserved = 0;
        Exception? caught = null;
        using var cts = new CancellationTokenSource(AntiHangTimeout);
        try
        {
            await foreach (var _ in conn.QueryAsync<ulong>("SELECT n FROM mock")
                .WithCancellation(cts.Token))
            {
                rowsObserved++;
            }
        }
        catch (Exception ex) { caught = ex; }

        _output.WriteLine($"Server-sends-more: rowsObserved={rowsObserved}, surface={caught?.GetType().FullName} — {caught?.Message}");

        // Pin: either we surface a typed exception OR the reader saw exactly the
        // declared count. Silent acceptance with corruption (extra rows) would be a bug.
        if (caught is null)
        {
            Assert.Equal(100, rowsObserved);
        }
        else
        {
            Assert.IsNotType<OutOfMemoryException>(caught);
            Assert.IsNotType<AccessViolationException>(caught);
        }
    }

    /// <summary>
    /// Builds a server Data message with one UInt64 column. Declared row count vs
    /// actual bytes-on-wire is the dimension under test.
    /// </summary>
    private static byte[] ComposeUInt64Block(int declaredRowCount, int providedRowCount)
    {
        var bw = new ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)ServerMessageType.Data);
        w.WriteString(string.Empty);            // table name
        BlockInfo.Default.Write(ref w);
        w.WriteVarInt(1);                       // num_columns
        w.WriteVarInt((ulong)declaredRowCount); // num_rows (declared)

        // Column header: name + type
        w.WriteString("n");
        w.WriteString("UInt64");

        // Column data: providedRowCount values, each 8 bytes LE.
        Span<byte> scratch = stackalloc byte[8];
        for (int i = 0; i < providedRowCount; i++)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(scratch, (ulong)i);
            w.WriteBytes(scratch);
        }

        return bw.WrittenMemory.ToArray();
    }
}
