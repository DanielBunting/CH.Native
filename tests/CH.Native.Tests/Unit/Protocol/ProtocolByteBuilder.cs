using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Tests.Unit.Protocol;

/// <summary>
/// Test helpers for synthesising wire-shaped byte sequences and for fragmenting
/// a contiguous payload across multiple <see cref="ReadOnlySequence{T}"/> segments.
///
/// <para>
/// Wire composition wraps <see cref="ProtocolWriter"/> wherever possible — the
/// less hand-rolled VarInt arithmetic in test code, the less drift when the wire
/// format evolves.
/// </para>
/// </summary>
internal static class ProtocolByteBuilder
{
    /// <summary>
    /// Compose a byte payload synchronously using a <see cref="ProtocolWriter"/>
    /// callback. The caller writes via the ref struct without capturing it across
    /// awaits or boundaries — just plain synchronous wire composition.
    /// </summary>
    public delegate void Compose(ref ProtocolWriter writer);

    public static byte[] Build(Compose compose)
    {
        var bw = new ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        compose(ref w);
        return bw.WrittenMemory.ToArray();
    }

    /// <summary>
    /// Wrap raw bytes as a single-segment <see cref="ReadOnlySequence{T}"/>.
    /// </summary>
    public static ReadOnlySequence<byte> AsSingleSegment(byte[] bytes)
        => new(bytes);

    /// <summary>
    /// Split <paramref name="bytes"/> into N <see cref="ReadOnlySequence{T}"/>
    /// segments of the given size (last segment may be shorter). Pass
    /// <paramref name="segmentSize"/>=1 to get the most fragmented possible
    /// sequence — useful for exercising SequenceReader segment-crossing.
    /// </summary>
    public static ReadOnlySequence<byte> AsFragmented(byte[] bytes, int segmentSize)
    {
        if (segmentSize <= 0) throw new ArgumentOutOfRangeException(nameof(segmentSize));
        if (bytes.Length == 0) return ReadOnlySequence<byte>.Empty;

        BufferSegment? first = null;
        BufferSegment? last = null;
        for (int offset = 0; offset < bytes.Length; offset += segmentSize)
        {
            int len = Math.Min(segmentSize, bytes.Length - offset);
            var seg = new BufferSegment(bytes.AsMemory(offset, len));
            if (first is null)
            {
                first = seg;
                last = seg;
            }
            else
            {
                last = last!.Append(seg);
            }
        }

        return new ReadOnlySequence<byte>(first!, 0, last!, last!.Memory.Length);
    }

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
