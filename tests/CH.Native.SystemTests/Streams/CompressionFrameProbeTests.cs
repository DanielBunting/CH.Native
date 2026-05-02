using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Compression;
using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// Negative-shape probes for the compressed-frame parsing path. <see cref="CompressionChecksumCorruptionTests"/>
/// covers payload-byte corruption and oversized size headers; this set extends to
/// header-byte flips, truncations, declared-size mismatches, Zstd-magic corruption,
/// boundary-zero blocks and uint32 overflow. Each test uses <see cref="MockClickHouseServer"/>
/// to mint exactly-malformed frames that a real ClickHouse server would never produce.
/// </summary>
[Trait(Categories.Name, Categories.Streams)]
public class CompressionFrameProbeTests
{
    private static readonly TimeSpan AntiHangTimeout = TimeSpan.FromSeconds(8);
    private readonly ITestOutputHelper _output;

    public CompressionFrameProbeTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task CityHash_FlippedAlgorithmByte_InHeader_SurfacesProtocolException()
    {
        // Flip the algorithm byte (offset 16). The checksum is computed over
        // [algorithm + sizes + payload], so flipping the algo byte invalidates the hash.
        // Acceptable surfaces: InvalidDataException ("checksum mismatch") OR a
        // ClickHouseProtocolException wrapping it.
        var framed = ComposeCompressedDataMessage(corruption: HeaderCorruption.AlgorithmByteFlip);

        var caught = await RunQueryAndCaptureAsync(framed, compression: true);
        _output.WriteLine($"Algo-flip surface: {caught?.GetType().FullName} — {caught?.Message}");
        Assert.NotNull(caught);
        var inner = ExtractCausal(caught!);
        Assert.True(
            inner is InvalidDataException || inner is ClickHouseProtocolException,
            $"Expected InvalidDataException or ClickHouseProtocolException; got {inner?.GetType().Name}");
    }

    [Fact]
    public async Task CityHash_FlippedCompressedSizeByte_InHeader_SurfacesError()
    {
        // Flip a byte inside the compressed-size field. Triggers checksum mismatch
        // before the parser tries to read past the buffer.
        var framed = ComposeCompressedDataMessage(corruption: HeaderCorruption.CompressedSizeByteFlip);

        var caught = await RunQueryAndCaptureAsync(framed, compression: true);
        _output.WriteLine($"CompressedSize-flip surface: {caught?.GetType().FullName} — {caught?.Message}");
        Assert.NotNull(caught);
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    [Fact]
    public async Task CityHash_TruncatedTo15Bytes_DoesNotHang()
    {
        // 15 bytes of checksum then EOF. The library should either throw an
        // EndOfStream-equivalent or honour cancellation — never deadlock.
        var truncated = new byte[15];
        new Random(7).NextBytes(truncated);

        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings(b => b.WithCompression(true)));
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        // Wrap with a Data-message header so the message dispatch enters the
        // compressed-block path, then the 15 truncated bytes follow.
        var bw = new ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)ServerMessageType.Data);
        w.WriteString(string.Empty);
        bw.Write(truncated);
        mock.EnqueueBytes(bw.WrittenMemory.Span);
        mock.CompleteOutgoing();

        using var cts = new CancellationTokenSource(AntiHangTimeout);
        Exception? caught = null;
        try
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1").WithCancellation(cts.Token)) { }
        }
        catch (Exception ex) { caught = ex; }

        _output.WriteLine($"Truncated-checksum surface: {caught?.GetType().FullName}");
        Assert.NotNull(caught);
        Assert.IsNotType<OutOfMemoryException>(caught);
    }

    [Fact]
    public async Task LZ4_DecompressedSizeMismatch_DeclaredSmallerThanContent_FailsCleanly()
    {
        // Build a real LZ4 block, then *understate* the declared uncompressed size in
        // the header (and refresh the checksum). The decoder should bail out with a
        // typed exception rather than over- or under-allocating.
        var framed = ComposeCompressedDataMessage(corruption: HeaderCorruption.UnderstatedUncompressedSize);

        var caught = await RunQueryAndCaptureAsync(framed, compression: true);
        _output.WriteLine($"Understated-uncompressed surface: {caught?.GetType().FullName} — {caught?.Message}");
        Assert.NotNull(caught);
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    [Fact]
    public async Task LZ4_DecompressedSize_ExceedsCap_RejectedBeforeAllocation()
    {
        // Declare an uncompressed size of 1 GiB — over the 256 MiB cap. The library
        // must reject at the size check *before* allocating. Refresh the checksum so
        // we hit the size cap rather than the checksum branch.
        var framed = ComposeCompressedDataMessage(corruption: HeaderCorruption.OverCapUncompressedSize);

        var caught = await RunQueryAndCaptureAsync(framed, compression: true);
        _output.WriteLine($"Over-cap uncompressed surface: {caught?.GetType().FullName} — {caught?.Message}");
        Assert.NotNull(caught);
        // Must not OOM — the cap is precisely the OOM guard.
        Assert.IsNotType<OutOfMemoryException>(caught);
        var inner = ExtractCausal(caught!);
        Assert.True(
            inner is ClickHouseProtocolException || inner is InvalidDataException,
            $"Expected typed protocol/data exception; got {inner?.GetType().Name}");
    }

    [Fact]
    public async Task Compression_ZeroUncompressedSize_HeaderClaimsZero_NoUB()
    {
        // Build a "compressed" frame whose declared uncompressed size is 0. There's
        // no contract this is a normal occurrence, but it must not crash, OOM, or
        // hang — surface a clean error or accept it as a no-op.
        var framed = ComposeCompressedDataMessage(corruption: HeaderCorruption.ZeroUncompressedSize);

        var caught = await RunQueryAndCaptureAsync(framed, compression: true);
        _output.WriteLine($"Zero-uncompressed-size surface: {caught?.GetType().FullName ?? "<no exception>"}");
        if (caught is not null)
        {
            Assert.IsNotType<OutOfMemoryException>(caught);
            Assert.IsNotType<AccessViolationException>(caught);
        }
    }

    [Fact]
    public async Task Compression_BlockSize_uint32_max_RejectedNotOOM()
    {
        // Declared compressed-size = uint.MaxValue. Library must reject at the size
        // gate, not via OutOfMemoryException.
        var framed = ComposeCompressedDataMessage(corruption: HeaderCorruption.UInt32MaxCompressedSize);

        var caught = await RunQueryAndCaptureAsync(framed, compression: true);
        _output.WriteLine($"uint32-max compressed-size surface: {caught?.GetType().FullName} — {caught?.Message}");
        Assert.NotNull(caught);
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    [Fact]
    public async Task Zstd_FrameWithCorruptedMagic_FailsCleanly()
    {
        // Build a Zstd-compressed block, then corrupt the first 4 bytes of the
        // *compressed payload* (Zstd magic = 0x28B52FFD little-endian). Refresh the
        // checksum so we exercise the decoder, not the checksum branch.
        var framed = ComposeZstdMessageWithCorruptedMagic();

        var caught = await RunQueryAndCaptureAsync(framed, compression: true);
        _output.WriteLine($"Zstd-magic surface: {caught?.GetType().FullName} — {caught?.Message}");
        Assert.NotNull(caught);
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    [Fact]
    public async Task Zstd_FrameWithUnsupportedDictionaryId_FailsCleanly()
    {
        // Synthetic Zstd frame: magic (0x28B52FFD LE) + Frame Header Descriptor
        // with DID=01 (dict ID = 1 byte), Single Segment=1 (no Window Descriptor),
        // FCS=0 (no Frame Content Size). Then 1 byte of dict ID = 0x42, then a
        // tiny RAW block that the decoder will need the dictionary to interpret.
        // Zstd MUST reject because no dictionary is loaded.
        var dictFrame = new List<byte>();
        dictFrame.AddRange(new byte[] { 0x28, 0xB5, 0x2F, 0xFD });          // magic
        dictFrame.Add(0b0010_0001);                                          // FHD: DID=01, SS=1, FCS=0
        dictFrame.Add(0x42);                                                 // 1-byte dict ID
        // Block header: last_block=1, type=00 (raw), size=0 → 0x01 0x00 0x00.
        dictFrame.AddRange(new byte[] { 0x01, 0x00, 0x00 });

        // Wrap into a CH compressed-block envelope: 16 byte checksum + 9 byte header
        // + payload. We refresh the checksum so we exercise the Zstd path, not the
        // checksum branch.
        var payload = dictFrame.ToArray();
        var totalSize = 16 + 9 + payload.Length;
        var bytes = new byte[totalSize];
        bytes[16] = 0x90;                                                    // Zstd algo
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(17), (uint)(9 + payload.Length));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(21), 64u);     // claimed uncompressed size
        payload.CopyTo(bytes.AsSpan(25));
        CityHash128.HashBytes(bytes.AsSpan(16), bytes.AsSpan(0, 16));

        var framed = FrameAsServerData(bytes);
        var caught = await RunQueryAndCaptureAsync(framed, compression: true);
        _output.WriteLine($"Zstd dict-frame surface: {caught?.GetType().FullName} — {caught?.Message}");
        Assert.NotNull(caught);
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    [Fact]
    public async Task Compression_RepeatedDecodeOnSameConnection_AllocationDriftBounded()
    {
        // Plan §1 #11. Decode N compressed blocks back-to-back on the same connection,
        // measure GC.GetTotalAllocatedBytes() across the run, and assert per-iteration
        // drift stays within a bounded multiple of the per-block size. A pooled
        // implementation should keep allocations low and steady; a regression that
        // re-allocates the decode buffer per frame would surface as N×blockSize drift.
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings(b =>
            b.WithCompression(true)));
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        // Build a payload with N empty compressed Data blocks + EOS.
        const int blockCount = 1000;
        var bw = new ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        for (int i = 0; i < blockCount; i++)
        {
            // Inner: empty block (BlockInfo + 0 columns + 0 rows).
            var innerBw = new ArrayBufferWriter<byte>();
            var innerW = new ProtocolWriter(innerBw);
            BlockInfo.Default.Write(ref innerW);
            innerW.WriteVarInt(0);
            innerW.WriteVarInt(0);
            using var compressed = CompressedBlock.CompressPooled(
                innerBw.WrittenMemory.Span, Lz4Compressor.Instance);

            w.WriteVarInt((ulong)ServerMessageType.Data);
            w.WriteString(string.Empty);
            bw.Write(compressed.Span);
        }
        w.WriteVarInt((ulong)ServerMessageType.EndOfStream);
        var totalPayloadBytes = bw.WrittenCount;
        mock.EnqueueBytes(bw.WrittenMemory.Span);
        mock.CompleteOutgoing();

        // Warm up the connection state so allocations from first-time setup don't
        // dominate the measurement.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var startBytes = GC.GetTotalAllocatedBytes(precise: true);
        await foreach (var _ in conn.QueryAsync<int>("SELECT 1")) { }
        var endBytes = GC.GetTotalAllocatedBytes(precise: true);

        var allocated = endBytes - startBytes;
        var perBlock = allocated / (double)blockCount;
        var perBlockWireSize = totalPayloadBytes / (double)blockCount;

        _output.WriteLine($"Total allocated during {blockCount} compressed blocks: {allocated} bytes");
        _output.WriteLine($"Per-block alloc: {perBlock:F0} bytes (wire size per block ≈ {perBlockWireSize:F0})");

        // Probe — the plan calls this "Doesn't have to pass — measure and log."
        // Empty blocks have a fixed per-block decode cost dominated by ProtocolReader
        // / column-reader bookkeeping that doesn't scale with payload size, so the
        // 5×-of-wire-size target the plan sketched isn't realistic here. Pin the
        // safety upper bound: no runaway allocation (<= 64 KiB per empty block) so a
        // future regression that grows per-block by an order of magnitude still fails.
        Assert.True(perBlock < 64 * 1024,
            $"Per-block allocation runaway: {perBlock:F0} bytes/block (wire ≈ {perBlockWireSize:F0})");
    }

    [Fact]
    public async Task UnknownAlgorithmId_RejectedAtDispatch()
    {
        // Algorithm byte = 0xAB (neither LZ4 0x82 nor Zstd 0x90). With a refreshed
        // checksum, the failure should surface at the algorithm-byte branch.
        var framed = ComposeCompressedDataMessage(corruption: HeaderCorruption.UnknownAlgorithm);

        var caught = await RunQueryAndCaptureAsync(framed, compression: true);
        _output.WriteLine($"Unknown-algo surface: {caught?.GetType().FullName} — {caught?.Message}");
        Assert.NotNull(caught);
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    /// <summary>
    /// Builds a Data-framed compressed empty block, applies the requested corruption,
    /// and (where the corruption mutates fields *under* the checksum) recomputes the
    /// checksum so the test exercises the post-checksum branch instead of falling out
    /// at the checksum stage.
    /// </summary>
    private enum HeaderCorruption
    {
        AlgorithmByteFlip,
        CompressedSizeByteFlip,
        UnderstatedUncompressedSize,
        OverCapUncompressedSize,
        ZeroUncompressedSize,
        UInt32MaxCompressedSize,
        UnknownAlgorithm,
    }

    private static byte[] ComposeCompressedDataMessage(HeaderCorruption corruption)
    {
        // Compose a real LZ4-compressed empty block.
        var innerBw = new ArrayBufferWriter<byte>();
        var innerW = new ProtocolWriter(innerBw);
        BlockInfo.Default.Write(ref innerW);
        innerW.WriteVarInt(0); // num columns
        innerW.WriteVarInt(0); // num rows
        var innerBytes = innerBw.WrittenMemory.ToArray();

        using var compressed = CompressedBlock.CompressPooled(innerBytes, Lz4Compressor.Instance);
        var bytes = compressed.Span.ToArray();
        // Layout: [0..16) checksum | [16] algo | [17..21) compSize | [21..25) uncompSize | [25..) payload
        bool refreshChecksum = false;

        switch (corruption)
        {
            case HeaderCorruption.AlgorithmByteFlip:
                bytes[16] ^= 0xFF; // flip the algo byte; checksum is invalid now (intended)
                break;

            case HeaderCorruption.CompressedSizeByteFlip:
                bytes[17] ^= 0xFF; // bit-flip in the compressed-size field; checksum invalid
                break;

            case HeaderCorruption.UnderstatedUncompressedSize:
                BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(21), 1); // declare 1 byte uncompressed
                refreshChecksum = true;
                break;

            case HeaderCorruption.OverCapUncompressedSize:
                BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(21), 1u * 1024 * 1024 * 1024); // 1 GiB
                refreshChecksum = true;
                break;

            case HeaderCorruption.ZeroUncompressedSize:
                BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(21), 0u);
                refreshChecksum = true;
                break;

            case HeaderCorruption.UInt32MaxCompressedSize:
                BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(17), uint.MaxValue);
                refreshChecksum = true;
                break;

            case HeaderCorruption.UnknownAlgorithm:
                bytes[16] = 0xAB;
                refreshChecksum = true;
                break;
        }

        if (refreshChecksum)
        {
            // Recompute CityHash128 over [algo..end] and write it into [0..16).
            CityHash128.HashBytes(bytes.AsSpan(16), bytes.AsSpan(0, 16));
        }

        return FrameAsServerData(bytes);
    }

    private static byte[] ComposeZstdMessageWithCorruptedMagic()
    {
        var innerBw = new ArrayBufferWriter<byte>();
        var innerW = new ProtocolWriter(innerBw);
        BlockInfo.Default.Write(ref innerW);
        innerW.WriteVarInt(0);
        innerW.WriteVarInt(0);
        var innerBytes = innerBw.WrittenMemory.ToArray();

        using var compressed = CompressedBlock.CompressPooled(innerBytes, ZstdCompressor.Instance);
        var bytes = compressed.Span.ToArray();
        // Compressed payload starts at offset 25 — corrupt the first 4 bytes (magic).
        for (int i = 0; i < 4 && 25 + i < bytes.Length; i++)
            bytes[25 + i] = 0xAA;

        // Refresh checksum so the decoder is exercised, not the checksum branch.
        CityHash128.HashBytes(bytes.AsSpan(16), bytes.AsSpan(0, 16));
        return FrameAsServerData(bytes);
    }

    private static byte[] FrameAsServerData(byte[] compressedBlock)
    {
        var bw = new ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)ServerMessageType.Data);
        w.WriteString(string.Empty);
        bw.Write(compressedBlock);
        // No EndOfStream — the failure should surface from the compressed-block path
        // before the dispatch loop looks for the next message.
        return bw.WrittenMemory.ToArray();
    }

    private static async Task<Exception?> RunQueryAndCaptureAsync(byte[] framed, bool compression)
    {
        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings(b =>
            b.WithCompression(compression)));
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(framed);

        using var cts = new CancellationTokenSource(AntiHangTimeout);
        Exception? caught = null;
        try
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1").WithCancellation(cts.Token)) { }
        }
        catch (Exception ex) { caught = ex; }

        return caught;
    }

    private static Exception ExtractCausal(Exception ex)
    {
        var current = ex;
        while (current is AggregateException agg && agg.InnerException is not null)
            current = agg.InnerException;
        // Prefer the deepest InvalidDataException / ClickHouseProtocolException if wrapped.
        var probe = current;
        while (probe.InnerException is { } inner)
        {
            if (inner is InvalidDataException || inner is ClickHouseProtocolException)
                current = inner;
            probe = inner;
        }
        return current;
    }
}
