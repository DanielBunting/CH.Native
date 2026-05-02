using System.Buffers.Binary;
using CH.Native.Compression;
using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Protocol;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// Pins the contract that <see cref="CompressedBlock"/>'s CityHash128 verification
/// rejects corrupted blocks deterministically. Existing compression tests round-trip
/// happy-path bytes; nothing today verifies that a corrupted block surfaces a clear
/// exception rather than silently producing garbage or — worse — allocating from
/// untrusted size fields and hanging or overflowing.
/// </summary>
[Trait(Categories.Name, Categories.Streams)]
public class CompressionChecksumCorruptionTests
{
    private static readonly TimeSpan AntiHangTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task CorruptedCompressedPayloadByte_ThrowsChecksumMismatch()
    {
        // Build a real LZ4 block, then flip one byte inside the compressed payload —
        // the checksum must reject it on the read path.
        var framed = ComposeCorruptedCompressedDataMessage(corruption: Corruption.PayloadByteFlip);

        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings(b =>
            b.WithCompression(true)));
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(framed);

        var query = Task.Run(async () =>
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1")) { }
        });

        var winner = await Task.WhenAny(query, Task.Delay(AntiHangTimeout));
        Assert.Same(query, winner);

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => query);
        var inner = ExtractCausalException(ex);
        Assert.True(
            inner is InvalidDataException ide && ide.Message.Contains("checksum mismatch", StringComparison.OrdinalIgnoreCase),
            $"Expected InvalidDataException with 'checksum mismatch'; got {inner?.GetType().Name}: {inner?.Message}");
    }

    [Fact]
    public async Task CorruptedCompressedSizeHeader_DoesNotInfiniteLoopOrOverread()
    {
        // Header CompressedSize = 0xFFFFFFFF. Two acceptable outcomes:
        //   (a) deterministic exception (InvalidDataException / OperationCanceled),
        //   (b) the read pump waits for never-arriving bytes — which must remain
        //       cancellable (no infinite loop, no allocation overflow).
        // Either way: no OutOfMemoryException, no hang past the cancel.
        var framed = ComposeCorruptedCompressedDataMessage(corruption: Corruption.OversizedSizeHeader);

        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings(b =>
            b.WithCompression(true)));
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(framed);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(800));
        Exception? caught = null;
        try
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1").WithCancellation(cts.Token)) { }
        }
        catch (Exception ex) { caught = ex; }

        Assert.NotNull(caught);

        // Crucially: not OOM, not AccessViolation. Anything in the family
        // {InvalidDataException, OperationCanceledException, ClickHouseProtocolException, IOException}
        // is acceptable — pin the inverse.
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    private enum Corruption
    {
        PayloadByteFlip,
        OversizedSizeHeader,
    }

    /// <summary>
    /// Builds a framed Data message containing a real LZ4-compressed empty block,
    /// then mutates one byte per the requested corruption mode. Returns the bytes
    /// to enqueue on the mock.
    /// </summary>
    private static byte[] ComposeCorruptedCompressedDataMessage(Corruption corruption)
    {
        // Inner uncompressed payload: BlockInfo + 0 columns + 0 rows.
        var innerBw = new System.Buffers.ArrayBufferWriter<byte>();
        var innerW = new ProtocolWriter(innerBw);
        BlockInfo.Default.Write(ref innerW);
        innerW.WriteVarInt(0);
        innerW.WriteVarInt(0);
        var innerBytes = innerBw.WrittenMemory.ToArray();

        // Real LZ4 compressed block with valid checksum + header.
        using var compressed = CompressedBlock.CompressPooled(innerBytes, Lz4Compressor.Instance);
        var compressedBytes = compressed.Span.ToArray();

        // Apply the corruption directly to the compressed-block bytes.
        switch (corruption)
        {
            case Corruption.PayloadByteFlip:
                // Compressed payload starts at offset 25 (16 checksum + 9 header).
                // Pick offset 25 if present; the smallest LZ4 frame is still ≥ 1 byte.
                int target = Math.Min(25, compressedBytes.Length - 1);
                compressedBytes[target] ^= 0xFF;
                break;
            case Corruption.OversizedSizeHeader:
                // Compressed size header is at offset 17 (16 checksum + 1 algorithm byte).
                BinaryPrimitives.WriteUInt32LittleEndian(compressedBytes.AsSpan(17), 0xFFFFFFFFu);
                break;
        }

        // Frame as a server Data message: Data + table name "" + compressed bytes + EOS.
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)ServerMessageType.Data);
        w.WriteString(string.Empty);
        w.WriteBytes(compressedBytes);
        w.WriteVarInt((ulong)ServerMessageType.EndOfStream);
        return bw.WrittenMemory.ToArray();
    }

    /// <summary>
    /// Walks AggregateException / InnerException chains so the caller can match against
    /// the actual root cause without tying tests to wrapping changes.
    /// </summary>
    private static Exception ExtractCausalException(Exception ex)
    {
        var current = ex;
        while (current is AggregateException agg && agg.InnerException is not null)
            current = agg.InnerException;
        while (current.InnerException is InvalidDataException ide)
            current = ide;
        // If outer is not InvalidDataException but inner is, prefer inner.
        if (current is not InvalidDataException && current.InnerException is InvalidDataException ide2)
            return ide2;
        return current;
    }
}
