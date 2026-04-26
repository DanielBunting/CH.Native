using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// Pins the boundary checks in <c>TryScanCompressedDataMessage</c> — the scan path
/// taken by the main pump when compression is negotiated. Pre-existing test
/// coverage in <c>MalformedServerTests</c> only exercises the uncompressed path
/// (mock builds settings with <c>WithCompression(false)</c> by default).
///
/// <para>The scan does three things in order:</para>
/// <list type="number">
///   <item>Read the table-name string (VarInt length + bytes) via <c>TrySkipString</c>
///         — same path as the uncompressed scan and the same <c>MaxStringLengthBytes</c>
///         cap applies. A malicious or malformed length must fail fast without
///         allocating a multi-GiB buffer.</item>
///   <item>Read the 16-byte CityHash128 checksum (peeked, not parsed).</item>
///   <item>Read the 9-byte compressed-block header (algorithm + compressed size).</item>
/// </list>
///
/// <para>These tests exercise the table-name length-prefix surface and the
/// "not enough bytes for a compressed header yet" branch. Building a real
/// LZ4 / ZSTD payload with valid CityHash128 isn't needed for the scan-boundary
/// surface — those defenses fire before any compression bytes are read.</para>
/// </summary>
[Trait(Categories.Name, Categories.Streams)]
public class CompressionScanBoundaryTests
{
    private static readonly TimeSpan AntiHangTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task CompressedScan_OversizedTableNameLength_ThrowsViaCap()
    {
        // Server declares Data with compression enabled, then sends a table-name
        // length-prefix of 64 MiB. The MaxStringLengthBytes cap (1 KiB here) must
        // fire on the scan pass — BEFORE any of the compressed payload bytes are
        // read, and certainly before any allocation.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings(b =>
            b.WithCompression(true).WithMaxStringLength(1024)));
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Data);
        w.WriteVarInt((ulong)(64 * 1024 * 1024)); // declared table-name length: 64 MiB
        // No payload — the cap fires first.
        mock.EnqueueBytes(bw.WrittenMemory.ToArray());

        var query = Task.Run(async () =>
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1")) { }
        });

        var winner = await Task.WhenAny(query, Task.Delay(AntiHangTimeout));
        Assert.Same(query, winner);

        var ex = await Assert.ThrowsAsync<ClickHouseProtocolException>(() => query);
        Assert.Contains("MaxStringLengthBytes", ex.Message);
        Assert.False(conn.CanBePooled);
    }

    [Fact]
    public async Task CompressedScan_TruncatedAfterTableName_WaitsForMoreBytes()
    {
        // Server sends Data + table name + only 10 of the required 25-byte
        // checksum-plus-header. The scan must return "wait for more bytes"
        // (not throw, not allocate). We then never send the rest and let
        // cancellation tear down the wait, proving no deadlock or premature
        // failure.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings(b =>
            b.WithCompression(true)));
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Data);
        w.WriteString("");                    // empty table name
        w.WriteBytes(new byte[10]);           // only 10 of needed 25 bytes after table name

        mock.EnqueueBytes(bw.WrittenMemory.ToArray());

        // Pump should NOT throw — it's waiting for more bytes. Drive a short
        // cancellation to verify the "wait" path is what's happening (vs an
        // immediate throw which would fail the test below).
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        OperationCanceledException? caught = null;
        try
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1").WithCancellation(cts.Token)) { }
        }
        catch (OperationCanceledException ex) { caught = ex; }

        Assert.NotNull(caught);
    }

    [Fact]
    public async Task CompressedScan_UnknownAlgorithmByte_HangsButHonoursCancellation()
    {
        // KNOWN GAP: when the compressed scan's algorithm peek finds something
        // other than 0x82 (LZ4) or 0x90 (ZSTD), it returns true and delegates
        // to the parser assuming "uncompressed". The parser then tries to read
        // block bytes that aren't actually buffered; the catch-IOException
        // backstop in the main pump treats this as "wait for more bytes" and
        // the query hangs because those bytes never arrive.
        //
        // This is a pre-existing fragility in the compressed-scan code path,
        // separate from the L1/L3/L4 hardening of Gap 3. The right fix is to
        // also run the uncompressed-block scan when the algorithm byte is
        // unknown, so the pump can distinguish "complete-but-uncompressed"
        // from "incomplete-and-corrupt". Logged here so the gap is visible.
        //
        // Until that fix lands, the test pins the only safe behaviour available:
        // the hang is at least cancellable.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings(b =>
            b.WithCompression(true)));
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Data);
        w.WriteString("");
        var header = new byte[25];
        new Random(0xABCDEF).NextBytes(header);
        header[16] = 0x55; // unknown algorithm — neither LZ4 (0x82) nor ZSTD (0x90)
        w.WriteBytes(header);
        mock.EnqueueBytes(bw.WrittenMemory.ToArray());

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        OperationCanceledException? caught = null;
        try
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1").WithCancellation(cts.Token)) { }
        }
        catch (OperationCanceledException ex) { caught = ex; }

        Assert.NotNull(caught);
    }

    [Fact]
    public async Task CompressedScan_LegitimateLZ4Block_RoundTripSucceeds()
    {
        // Positive control: build a real LZ4-compressed Data message with a valid
        // CityHash128 checksum — proves the scan path's "happy path" still works
        // when the wire bytes are well-formed. If this test breaks while the
        // uncompressed equivalent passes, the regression is in the compressed
        // scan / parse code path specifically.
        //
        // The compressed payload is an empty Data block (table name "" + BlockInfo
        // + 0 columns + 0 rows) — the smallest valid uncompressed payload to
        // wrap in compression.
        var revision = MockClickHouseServer.PinnedProtocolRevision;

        // Build the uncompressed inner block bytes (excluding the outer table name
        // — that goes outside the compressed envelope).
        var innerBw = new System.Buffers.ArrayBufferWriter<byte>();
        var innerW = new ProtocolWriter(innerBw);
        Data.BlockInfo.Default.Write(ref innerW);
        innerW.WriteVarInt(0);
        innerW.WriteVarInt(0);
        var innerBytes = innerBw.WrittenMemory.ToArray();

        // Compress with LZ4. Use the library's own helper to produce wire-correct
        // bytes including the 9-byte header and the CityHash128 checksum.
        var compressor = Compression.Lz4Compressor.Instance;
        using var compressedBlock = Compression.CompressedBlock.CompressPooled(innerBytes, compressor);
        // CompressPooled returns the on-wire framing: 16-byte checksum + 9-byte header + payload.
        var compressedBytes = compressedBlock.Span.ToArray();

        // Followed by EOS so the pump knows the response is over.
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.Data);
        w.WriteString(string.Empty);                 // table name (outside the compressed envelope)
        w.WriteBytes(compressedBytes);
        w.WriteVarInt((ulong)Protocol.ServerMessageType.EndOfStream);

        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings(b =>
            b.WithCompression(true)));
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(bw.WrittenMemory.ToArray());

        var query = Task.Run(async () =>
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1")) { }
        });

        var winner = await Task.WhenAny(query, Task.Delay(AntiHangTimeout));
        Assert.Same(query, winner);
        await query;
        Assert.True(conn.CanBePooled);
    }
}
