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
/// Pins the wire-side contract for compression-algorithm mismatch and unknown
/// algorithm IDs.
///
/// <para>
/// <see cref="CompressedBlock.GetCompressor(byte)"/> hard-codes the supported
/// algorithm bytes (<c>0x82=LZ4, 0x90=Zstd</c>) and throws
/// <see cref="NotSupportedException"/> on anything else. The question this file
/// answers: when a (real or rogue) server sends a block with an unsupported
/// algorithm ID, does the failure surface cleanly to the caller and poison the
/// connection (rather than silently producing garbage)?
/// </para>
///
/// <para>
/// Also: when the client negotiates LZ4 but the server unexpectedly sends Zstd
/// (or vice versa), the failure mode is identical — the algorithm byte in the
/// block header drives <c>GetCompressor</c>, not any negotiated session state.
/// This locks in that "no negotiated state" model so a future refactor that
/// added per-session compression locking would visibly flip the test.
/// </para>
/// </summary>
[Trait(Categories.Name, Categories.Streams)]
public class CompressionAlgorithmMismatchTests
{
    private static readonly TimeSpan AntiHangTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task UnknownAlgorithmId_FailsCleanly_WithTypedException()
    {
        // Build a real LZ4 block, then overwrite the algorithm byte with 0xAB
        // (neither LZ4 0x82 nor Zstd 0x90). Recompute the checksum so the gate
        // doesn't reject earlier — we want the unknown-algorithm error path.
        //
        // Observed contract: the failure does NOT surface as the
        // NotSupportedException thrown by GetCompressor — the read pump's
        // multi-chunk recovery loop (ClickHouseConnection.cs:1759-1779)
        // catches InvalidOperationException and re-reads the next compressed
        // chunk. With a 1-chunk wire payload this means the pump eventually
        // tries to interpret the next byte as a server message type, where
        // it lands on an unknown / Log type and surfaces a typed
        // ClickHouseProtocolException. EITHER outcome (the typed
        // NotSupportedException directly OR the wrapping protocol exception)
        // is acceptable; what's NOT acceptable is silent success or OOM.
        var framed = ComposeMutatedAlgorithmDataMessage(newAlgorithmId: 0xAB);

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
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);

        var nse = FindException<NotSupportedException>(caught);
        var protoEx = FindException<CH.Native.Exceptions.ClickHouseProtocolException>(caught);
        Assert.True(nse is not null || protoEx is not null,
            $"Expected NotSupportedException or ClickHouseProtocolException; got {caught.GetType().Name}: {caught.Message}");

        // Either way: the connection is tainted.
        Assert.False(conn.CanBePooled,
            "unknown compression algorithm must poison the connection (not return it to the pool)");
    }

    [Fact]
    public async Task ZstdAlgorithmByteOnLz4ConfiguredClient_StillRoundTrips()
    {
        // The client doesn't lock to a specific server-side algorithm — block
        // headers carry the algorithm byte and GetCompressor selects per-block.
        // So a block compressed with Zstd is decoded by the Zstd codec even if
        // the client was configured with WithCompression(LZ4) for its outbound
        // writes. Pin this lookup-per-block behavior.
        var framed = ComposeFreshlyCompressedDataMessage(ZstdCompressor.Instance);

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
        await query; // surface any exception
    }

    private static T? FindException<T>(Exception? ex) where T : Exception
    {
        var current = ex;
        while (current is not null)
        {
            if (current is T match) return match;
            if (current is AggregateException agg)
            {
                foreach (var inner in agg.InnerExceptions)
                {
                    var found = FindException<T>(inner);
                    if (found is not null) return found;
                }
            }
            current = current.InnerException;
        }
        return null;
    }

    private static byte[] ComposeMutatedAlgorithmDataMessage(byte newAlgorithmId)
    {
        var innerBw = new System.Buffers.ArrayBufferWriter<byte>();
        var innerW = new ProtocolWriter(innerBw);
        BlockInfo.Default.Write(ref innerW);
        innerW.WriteVarInt(0);
        innerW.WriteVarInt(0);
        var innerBytes = innerBw.WrittenMemory.ToArray();

        using var compressed = CompressedBlock.CompressPooled(innerBytes, Lz4Compressor.Instance);
        var bytes = compressed.Span.ToArray();

        // Layout: [0..16) checksum, [16] algorithm, [17..21) compressedSize,
        //         [21..25) uncompressedSize, [25..) compressed payload.
        bytes[16] = newAlgorithmId;
        // Recompute the checksum so the byte-flip survives the gate; we want
        // the unknown-algorithm error path, not the checksum path.
        CityHash128.HashBytes(bytes.AsSpan(16), bytes.AsSpan(0, 16));

        return WrapAsDataMessage(bytes);
    }

    private static byte[] ComposeFreshlyCompressedDataMessage(ICompressor compressor)
    {
        var innerBw = new System.Buffers.ArrayBufferWriter<byte>();
        var innerW = new ProtocolWriter(innerBw);
        BlockInfo.Default.Write(ref innerW);
        innerW.WriteVarInt(0);
        innerW.WriteVarInt(0);
        var innerBytes = innerBw.WrittenMemory.ToArray();

        using var compressed = CompressedBlock.CompressPooled(innerBytes, compressor);
        return WrapAsDataMessage(compressed.Span.ToArray());
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
