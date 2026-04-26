using System.Buffers;
using CH.Native.Data;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

/// <summary>
/// Shared static helpers for column-skipper round-trip tests. ProtocolReader is a
/// ref struct so all helpers operate via direct calls, not callbacks.
/// </summary>
internal static class SkipperTestBase
{
    public delegate void Emit(ref ProtocolWriter writer);

    public static ReadOnlySequence<byte> Encode(Emit emit)
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        emit(ref writer);
        return new ReadOnlySequence<byte>(buf.WrittenMemory);
    }

    /// <summary>
    /// Splits a single-segment sequence into multiple linked segments at the given byte
    /// offsets. Used to exercise reads that straddle pipe segment boundaries.
    /// </summary>
    public static ReadOnlySequence<byte> Fragment(ReadOnlySequence<byte> source, params int[] segmentSizes)
    {
        var data = source.ToArray();
        if (segmentSizes.Sum() != data.Length)
            throw new ArgumentException(
                $"Segment sizes {string.Join("+", segmentSizes)} = {segmentSizes.Sum()} do not match buffer length {data.Length}.",
                nameof(segmentSizes));

        BufferSegment? first = null;
        BufferSegment? last = null;
        int offset = 0;
        foreach (var size in segmentSizes)
        {
            var segment = new BufferSegment(new ReadOnlyMemory<byte>(data, offset, size));
            if (first is null)
            {
                first = segment;
                last = segment;
            }
            else
            {
                last = last!.Append(segment);
            }
            offset += size;
        }

        return new ReadOnlySequence<byte>(first!, 0, last!, last!.Memory.Length);
    }

    /// <summary>
    /// Returns a copy of <paramref name="source"/> truncated to <paramref name="byteCount"/>
    /// bytes. Used to construct partial-stream inputs for `Try*` failure tests.
    /// </summary>
    public static ReadOnlySequence<byte> Truncate(ReadOnlySequence<byte> source, int byteCount)
    {
        if (byteCount < 0 || byteCount > source.Length)
            throw new ArgumentOutOfRangeException(nameof(byteCount));
        return source.Slice(0, byteCount);
    }

    /// <summary>
    /// Asserts that the skipper consumes exactly the same number of bytes as the reader
    /// would consume reading the same column. Both invocations start from the same byte
    /// position; <paramref name="readPrefix"/>/<paramref name="skipPrefix"/> are invoked
    /// before the column body.
    /// </summary>
    public static void AssertParity(
        ReadOnlySequence<byte> bytes,
        int rowCount,
        ReaderPrefix readPrefix,
        ReaderColumn readColumn,
        SkipperColumn skipColumn)
    {
        var totalBytes = bytes.Length;

        var readerR = new ProtocolReader(bytes);
        readPrefix(ref readerR);
        readColumn(ref readerR, rowCount);
        var readerConsumed = totalBytes - readerR.Remaining;

        var readerS = new ProtocolReader(bytes);
        Assert.True(skipColumn(ref readerS, rowCount), "Skipper returned false on a valid stream.");
        var skipperConsumed = totalBytes - readerS.Remaining;

        Assert.Equal(readerConsumed, skipperConsumed);
    }

    public delegate void ReaderPrefix(ref ProtocolReader reader);
    public delegate void ReaderColumn(ref ProtocolReader reader, int rowCount);
    public delegate bool SkipperColumn(ref ProtocolReader reader, int rowCount);

    private sealed class BufferSegment : ReadOnlySequenceSegment<byte>
    {
        public BufferSegment(ReadOnlyMemory<byte> memory)
        {
            Memory = memory;
        }

        public BufferSegment Append(BufferSegment next)
        {
            next.RunningIndex = RunningIndex + Memory.Length;
            Next = next;
            return next;
        }
    }
}
