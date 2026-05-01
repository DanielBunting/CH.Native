using System.Buffers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Pre-fix <see cref="FixedStringColumnWriter.WriteValue"/> silently truncated
/// oversized input to the column's declared length. The user got no
/// diagnostic; the truncated bytes were committed to the table and the trail
/// of where data went was gone. The new contract throws
/// <see cref="ArgumentException"/> on overflow; equal-or-shorter input still
/// zero-pads as before.
/// </summary>
public class FixedStringColumnWriterTruncationTests
{
    [Fact]
    public void WriteValue_OversizedInput_Throws()
    {
        var w = new FixedStringColumnWriter(length: 4);
        var bw = new ArrayBufferWriter<byte>();
        Assert.Throws<ArgumentException>(() =>
        {
            var pw = new ProtocolWriter(bw);
            w.WriteValue(ref pw, new byte[] { 1, 2, 3, 4, 5 });
        });
    }

    [Fact]
    public void WriteValue_ExactSize_RoundTripsWithoutPadding()
    {
        var w = new FixedStringColumnWriter(length: 4);
        var bw = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(bw);
        w.WriteValue(ref pw, new byte[] { 1, 2, 3, 4 });
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, bw.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteValue_ShortInput_ZeroPadsToLength()
    {
        var w = new FixedStringColumnWriter(length: 4);
        var bw = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(bw);
        w.WriteValue(ref pw, new byte[] { 0xAA });
        Assert.Equal(new byte[] { 0xAA, 0, 0, 0 }, bw.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteValue_EmptyInput_AllZeros()
    {
        var w = new FixedStringColumnWriter(length: 3);
        var bw = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(bw);
        w.WriteValue(ref pw, Array.Empty<byte>());
        Assert.Equal(new byte[] { 0, 0, 0 }, bw.WrittenSpan.ToArray());
    }
}
