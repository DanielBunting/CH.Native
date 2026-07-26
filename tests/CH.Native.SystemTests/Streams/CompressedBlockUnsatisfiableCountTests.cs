using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using CH.Native.Compression;
using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// Pins the terminal-failure contract of the compressed multi-chunk accumulate
/// loop (<c>ClickHouseConnection.ReadCompressedTypedBlock</c>).
///
/// <para>
/// That loop decompresses chunks and re-attempts the block parse as data grows,
/// treating "declared counts exceed bytes decompressed so far"
/// (<c>ClickHouseCountGuardException</c>) as the expected first-pass state of a
/// legitimate large block — see <see cref="CompressedMultiChunkBlockTests"/> for
/// the well-formed case this retry contract exists to serve.
/// </para>
///
/// <para>
/// The hazard is the exit path. When the chunks run out with the parse still
/// unsatisfied, the loop must distinguish two states that look alike:
/// <list type="bullet">
/// <item><description><b>Truncation</b> — more chunks are still in flight. Correct
/// response: <c>InvalidOperationException</c>, which the pump's scan loop reads as
/// "need more data" and retries once more bytes arrive.</description></item>
/// <item><description><b>Unsatisfiable</b> — the server has sent everything and the
/// declared counts were bogus (the corrupt/hostile case
/// <c>ProtocolGuards.ValidateBlockHeaderCounts</c> exists to defend against).
/// Correct response: terminal <c>ClickHouseProtocolException</c>.</description></item>
/// </list>
/// Misclassifying the second as the first makes the pump wait for bytes that will
/// never arrive: the query hangs until CommandTimeout instead of failing fast, and
/// every retry re-decompresses the whole accumulated chunk set.
/// </para>
///
/// <para>
/// A real ClickHouse server won't emit these frames, so these drive
/// <see cref="MockClickHouseServer"/> rather than a container.
/// </para>
/// </summary>
[Trait(Categories.Name, Categories.Streams)]
public sealed class CompressedBlockUnsatisfiableCountTests
{
    // Long enough that a healthy fail-fast never trips it, short enough that a
    // genuine hang is caught well inside the test-run budget.
    private static readonly TimeSpan AntiHangTimeout = TimeSpan.FromSeconds(6);

    private static readonly int[] ExpectedValues = { 7, 8, 9, 10 };

    [Fact]
    public async Task UnsatisfiableColumnCount_SocketStaysOpen_FailsFastInsteadOfHanging()
    {
        // The critical case. A single compressed chunk declares 1,000,000 columns
        // (minimum footprint 2,000,000 bytes) but carries only a handful of
        // decompressed bytes, and is followed by EndOfStream — a ONE-byte tail.
        //
        // The count guard fires retryably, the loop finds no further chunk, and
        // the exit test `reader.Remaining < 17` is satisfied by that 1-byte tail
        // (17 = the bytes needed to recognise a chunk header). So the block is
        // reported as "need more data" even though the response is complete.
        //
        // The mock deliberately does NOT close the socket — a real server holds
        // the connection open for the next query, so nothing ever wakes the pump.
        var framed = ComposeUnsatisfiableColumnCountDataMessage(declaredColumnCount: 1_000_000);

        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings(b => b.WithCompression(true)));
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(framed);
        // No CompleteOutgoing(): socket stays open, exactly as a real server would.

        using var cts = new CancellationTokenSource(AntiHangTimeout);
        var sw = Stopwatch.StartNew();
        Exception? caught = null;
        try
        {
            await foreach (var _ in conn.QueryStreamAsync<int>("SELECT 1").WithCancellation(cts.Token)) { }
        }
        catch (Exception ex) { caught = ex; }
        sw.Stop();

        Assert.False(cts.IsCancellationRequested,
            $"Read hung for {sw.Elapsed.TotalSeconds:F1}s on a COMPLETE response with unsatisfiable " +
            "declared counts. The accumulate loop classified 'server sent everything and the counts " +
            "were bogus' as 'more chunks are coming'.");

        Assert.NotNull(caught);
        var protoEx = FindException<ClickHouseProtocolException>(caught);
        Assert.True(protoEx is not null,
            $"Expected a terminal ClickHouseProtocolException; got {caught!.GetType().Name}: {caught.Message}");
        Assert.False(conn.CanBePooled,
            "A block that could not be parsed leaves the wire at an unknown offset — the connection must be poisoned.");
    }

    [Fact]
    public async Task LegitimateBlock_ChunksDribbledLate_StillParses()
    {
        // Regression guard for the retry contract the exit path must preserve.
        // A well-formed two-chunk block is delivered with the second chunk
        // arriving only after the client has already consumed the first — the
        // exact state ("chunks ran out mid-parse") that the unsatisfiable case
        // above must be distinguished FROM, not lumped in with. A fix that makes
        // the exit terminal unconditionally would break this test.
        var (firstChunk, rest) = ComposeSplitTwoChunkDataMessage();

        await using var mock = new MockClickHouseServer();
        mock.Start();

        await using var conn = new ClickHouseConnection(mock.BuildSettings(b => b.WithCompression(true)));
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(firstChunk);

        // Release the tail only after the client has had time to consume chunk 1
        // and park in the "need more data" retry.
        _ = Task.Run(async () =>
        {
            await Task.Delay(300);
            mock.EnqueueBytes(rest);
        });

        using var cts = new CancellationTokenSource(AntiHangTimeout);
        var values = new List<int>();
        await using (var reader = await conn.ExecuteReaderAsync("SELECT 1", cts.Token))
        {
            while (await reader.ReadAsync(cts.Token))
                values.Add(reader.GetInt32(0));
        }

        // Exact values, not just the row count: a chunk boundary that fell inside
        // a value must be stitched back together, not silently zero-filled.
        Assert.Equal(ExpectedValues, values);
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

    /// <summary>
    /// One compressed chunk whose inner block header declares
    /// <paramref name="declaredColumnCount"/> columns but carries no column data,
    /// framed as a Data message and followed by EndOfStream. The checksum is
    /// genuine, so the frame clears every gate ahead of the block parser.
    /// </summary>
    private static byte[] ComposeUnsatisfiableColumnCountDataMessage(int declaredColumnCount)
    {
        var innerBw = new ArrayBufferWriter<byte>();
        var innerW = new ProtocolWriter(innerBw);
        BlockInfo.Default.Write(ref innerW);
        innerW.WriteVarInt((ulong)declaredColumnCount); // column count — unsatisfiable
        innerW.WriteVarInt(0);                          // row count
        var innerBytes = innerBw.WrittenMemory.ToArray();

        using var compressed = CompressedBlock.CompressPooled(innerBytes, Lz4Compressor.Instance);

        var bw = new ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)ServerMessageType.Data);
        w.WriteString(string.Empty);
        w.WriteBytes(compressed.Span);
        w.WriteVarInt((ulong)ServerMessageType.EndOfStream); // one-byte tail
        return bw.WrittenMemory.ToArray();
    }

    /// <summary>
    /// A well-formed single-column UInt32 block split across two compressed
    /// chunks, returned as (bytes up to and including chunk 1, remaining bytes).
    /// The split point is chosen so the client can fully consume chunk 1 — and
    /// fail to complete the parse from it — before the tail arrives.
    /// </summary>
    /// <summary>
    /// Serialized bytes of a well-formed single-column Int32 block carrying
    /// <see cref="ExpectedValues"/> — the payload a compressed chunk carries once
    /// decompressed.
    /// </summary>
    private static byte[] ComposeInnerBlockBytes()
    {
        var innerBw = new ArrayBufferWriter<byte>();
        var innerW = new ProtocolWriter(innerBw);
        BlockInfo.Default.Write(ref innerW);
        innerW.WriteVarInt(1); // 1 column
        innerW.WriteVarInt((ulong)ExpectedValues.Length);
        innerW.WriteString("n");
        innerW.WriteString("Int32");
        innerW.WriteByte(0); // has-custom-serialization flag (protocol >= WithCustomSerialization)

        Span<byte> scratch = stackalloc byte[4];
        foreach (var v in ExpectedValues)
        {
            BinaryPrimitives.WriteInt32LittleEndian(scratch, v);
            innerW.WriteBytes(scratch);
        }
        return innerBw.WrittenMemory.ToArray();
    }

    private static (byte[] First, byte[] Tail) ComposeSplitTwoChunkDataMessage()
    {
        // ClickHouse splits a block's serialized bytes across chunks at arbitrary
        // offsets; the client's accumulator concatenates decompressed chunks and
        // re-parses. Mirror that: serialize the block, cut it in half, compress
        // each half as its own chunk.
        var innerBytes = ComposeInnerBlockBytes();

        var half = innerBytes.Length / 2;
        using var chunk1 = CompressedBlock.CompressPooled(innerBytes.AsSpan(0, half), Lz4Compressor.Instance);
        using var chunk2 = CompressedBlock.CompressPooled(innerBytes.AsSpan(half), Lz4Compressor.Instance);

        var firstBw = new ArrayBufferWriter<byte>();
        var firstW = new ProtocolWriter(firstBw);
        firstW.WriteVarInt((ulong)ServerMessageType.Data);
        firstW.WriteString(string.Empty);
        firstW.WriteBytes(chunk1.Span);

        var restBw = new ArrayBufferWriter<byte>();
        var restW = new ProtocolWriter(restBw);
        restW.WriteBytes(chunk2.Span);
        restW.WriteVarInt((ulong)ServerMessageType.EndOfStream);

        return (firstBw.WrittenMemory.ToArray(), restBw.WrittenMemory.ToArray());
    }
}
