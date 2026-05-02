using System.Buffers.Binary;
using CH.Native.Compression;
using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Protocol;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// Pins the failure mode when a compressed block's <em>uncompressed_size</em>
/// header overstates the actual decompressed payload (the "underrun" case).
///
/// <para>
/// <see cref="CompressionChecksumCorruptionTests"/> covers (a) byte-flip in the
/// compressed payload — checksum rejects, and (b) oversize <c>compressedSize</c>
/// header — checksum still rejects (its scope covers all post-checksum bytes).
/// What's not yet pinned: a hostile/corrupt server that <em>recomputes</em> a
/// valid CityHash128 over a header where <c>uncompressedSize</c> is larger than
/// the LZ4 codec will actually produce. The checksum gate doesn't catch this;
/// only the downstream block parser would notice the trailing junk.
/// </para>
///
/// <para>
/// Today's behavior (per <c>CompressedBlock.cs:151,194</c>): a buffer of size
/// <c>uncompressedSize</c> is allocated and <c>compressor.Decompress</c> is
/// called without verifying its return value. If LZ4 writes fewer bytes than
/// claimed, the trailing bytes of the buffer are whatever the rented array
/// previously held. The downstream block parser sees junk and the call surfaces
/// some kind of typed exception — either a protocol error or a value-shape
/// failure. This test pins "fails cleanly, does not silently corrupt the next
/// query, does not OOM."
/// </para>
/// </summary>
[Trait(Categories.Name, Categories.Streams)]
public class CompressionUnderrunTests
{
    private static readonly TimeSpan AntiHangTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task UncompressedSizeOverstated_ThrowsTypedProtocolException()
    {
        // CompressedBlock.Decompress / DecompressPooled now compare the
        // decompressor's actual output length against the wire-declared
        // <c>uncompressed_size</c>. An overstated header — even one that
        // would otherwise let trailing pool bytes silently bleed into a
        // small block parse — surfaces a typed
        // <see cref="ClickHouseProtocolException"/>.
        var framed = ComposeUncompressedSizeOverstatedDataMessage(overstateBy: 256);

        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings(b =>
            b.WithCompression(true)));
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(framed);
        mock.CompleteOutgoing();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        Exception? caught = null;
        try
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1").WithCancellation(cts.Token)) { }
        }
        catch (Exception ex) { caught = ex; }

        Assert.NotNull(caught);
        var protoEx = FindException<CH.Native.Exceptions.ClickHouseProtocolException>(caught);
        Assert.True(protoEx is not null,
            $"Expected ClickHouseProtocolException; got {caught.GetType().Name}: {caught.Message}");
        Assert.Contains("wrote", protoEx!.Message, StringComparison.OrdinalIgnoreCase);
        Assert.False(conn.CanBePooled, "underrun must poison the connection");
    }

    private static T? FindException<T>(Exception? ex) where T : Exception
    {
        var current = ex;
        while (current is not null)
        {
            if (current is T match) return match;
            current = current.InnerException;
        }
        return null;
    }

    [Fact]
    public async Task UncompressedSizeUnderstated_AlsoFailsCleanly()
    {
        // The mirror case: claim *fewer* bytes than the LZ4 codec will produce.
        // LZ4Codec.Decode would either truncate output or write past the
        // destination span (which would throw ArgumentOutOfRangeException /
        // similar). This pins that the failure surfaces typed.
        var framed = ComposeUncompressedSizeUnderstatedDataMessage(understateTo: 1);

        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings(b =>
            b.WithCompression(true)));
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(framed);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        Exception? caught = null;
        try
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1").WithCancellation(cts.Token)) { }
        }
        catch (Exception ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    /// <summary>
    /// Builds a server-side Data message whose embedded compressed block has a
    /// correctly-checksummed header where <c>uncompressed_size</c> is artificially
    /// inflated by <paramref name="overstateBy"/> bytes.
    /// </summary>
    private static byte[] ComposeUncompressedSizeOverstatedDataMessage(int overstateBy)
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
        var bytes = compressed.Span.ToArray();

        // Layout: [0..16) checksum, [16] algorithm, [17..21) compressedSize,
        //         [21..25) uncompressedSize, [25..) compressed payload.
        var realUncompressed = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(21));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(21), realUncompressed + (uint)overstateBy);

        // Recompute CityHash128 over the post-checksum span so the gate accepts
        // the (now-mutated) header. Without this, the test would only assert on
        // the existing checksum-mismatch path.
        CityHash128.HashBytes(bytes.AsSpan(16), bytes.AsSpan(0, 16));

        return WrapAsDataMessage(bytes);
    }

    private static byte[] ComposeUncompressedSizeUnderstatedDataMessage(int understateTo)
    {
        var innerBw = new System.Buffers.ArrayBufferWriter<byte>();
        var innerW = new ProtocolWriter(innerBw);
        BlockInfo.Default.Write(ref innerW);
        innerW.WriteVarInt(0);
        innerW.WriteVarInt(0);
        var innerBytes = innerBw.WrittenMemory.ToArray();

        using var compressed = CompressedBlock.CompressPooled(innerBytes, Lz4Compressor.Instance);
        var bytes = compressed.Span.ToArray();

        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(21), (uint)understateTo);
        CityHash128.HashBytes(bytes.AsSpan(16), bytes.AsSpan(0, 16));

        return WrapAsDataMessage(bytes);
    }

    private static byte[] WrapAsDataMessage(byte[] compressedBlockBytes)
    {
        var bw = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)ServerMessageType.Data);
        w.WriteString(string.Empty);
        w.WriteBytes(compressedBlockBytes);
        w.WriteVarInt((ulong)ServerMessageType.EndOfStream);
        return bw.WrittenMemory.ToArray();
    }
}
