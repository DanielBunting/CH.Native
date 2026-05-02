using System.Buffers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Pre-fix the writer silently clamped <c>DateOnly</c> values past 2149-06-06
/// (ClickHouse <c>Date</c>'s UInt16 saturation point) and before 1970-01-01
/// (the epoch floor). Round-trip then produced the wrong year with no
/// diagnostic — the user's "2200-01-01" landed as "2149-06-06" in the table.
/// The fix throws <see cref="ArgumentOutOfRangeException"/> and points the
/// caller at <c>Date32</c>, which has the wider range.
/// </summary>
public class DateColumnWriterRangeTests
{
    [Fact]
    public void WriteValue_DateAfterSaturation_Throws()
    {
        // 2149-06-07 is one day past Date's max representable value.
        var writer = new DateColumnWriter();
        var bw = new ArrayBufferWriter<byte>();
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            var pw = new ProtocolWriter(bw);
            writer.WriteValue(ref pw, new DateOnly(2149, 6, 7));
        });
        Assert.Contains("Date32", ex.Message);
    }

    [Fact]
    public void WriteValue_DateBeforeEpoch_Throws()
    {
        var writer = new DateColumnWriter();
        var bw = new ArrayBufferWriter<byte>();
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            var pw = new ProtocolWriter(bw);
            writer.WriteValue(ref pw, new DateOnly(1969, 12, 31));
        });
    }

    [Fact]
    public void WriteValue_AtEpoch_RoundTripsAsZero()
    {
        var writer = new DateColumnWriter();
        var bw = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(bw);
        writer.WriteValue(ref pw, new DateOnly(1970, 1, 1));
        Assert.Equal(new byte[] { 0, 0 }, bw.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteValue_AtSaturation_RoundTripsAsMaxUshort()
    {
        var writer = new DateColumnWriter();
        var bw = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(bw);
        writer.WriteValue(ref pw, new DateOnly(2149, 6, 6)); // exactly UInt16.MaxValue days
        Assert.Equal(new byte[] { 0xFF, 0xFF }, bw.WrittenSpan.ToArray());
    }
}
