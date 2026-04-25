using System.Buffers;
using CH.Native.BulkInsert;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

public class DateTime64ExtractorTests
{
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Sample =
        new DateTime(2024, 1, 15, 12, 30, 45, DateTimeKind.Utc).AddTicks(1_234_567);

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    public void DateTime64_WritesExactInt64_NoDoublePrecisionLoss(int precision)
    {
        var property = typeof(DateTimeRow).GetProperty(nameof(DateTimeRow.Value))!;
        var extractor = ColumnExtractorFactory.Create<DateTimeRow>(property, "value", $"DateTime64({precision})");

        var rows = new List<DateTimeRow> { new() { Value = Sample } };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        extractor.ExtractAndWrite(ref writer, rows, 1);

        Assert.Equal(8, buffer.WrittenCount);
        Assert.Equal(ExpectedWire(Sample, precision), BitConverter.ToInt64(buffer.WrittenSpan));
    }

    [Theory]
    [InlineData(8)]
    [InlineData(9)]
    public void NullableDateTime64_HighPrecision_WritesExactInt64(int precision)
    {
        var property = typeof(NullableDateTimeRow).GetProperty(nameof(NullableDateTimeRow.Value))!;
        var extractor = ColumnExtractorFactory.Create<NullableDateTimeRow>(property, "value", $"Nullable(DateTime64({precision}))");

        var rows = new List<NullableDateTimeRow>
        {
            new() { Value = Sample },
            new() { Value = null },
        };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        extractor.ExtractAndWrite(ref writer, rows, 2);

        // Layout: 2 null bytes, then 2×Int64 values
        var span = buffer.WrittenSpan;
        Assert.Equal(2 + 2 * 8, span.Length);
        Assert.Equal(0x00, span[0]); // not null
        Assert.Equal(0x01, span[1]); // null
        Assert.Equal(ExpectedWire(Sample, precision), BitConverter.ToInt64(span[2..10]));
        Assert.Equal(0L, BitConverter.ToInt64(span[10..18]));
    }

    [Theory]
    [InlineData(3)]
    [InlineData(6)]
    [InlineData(8)]
    [InlineData(9)]
    public void DateTimeOffset_DateTime64_WritesExactInt64(int precision)
    {
        var property = typeof(DateTimeOffsetRow).GetProperty(nameof(DateTimeOffsetRow.Value))!;
        var extractor = ColumnExtractorFactory.Create<DateTimeOffsetRow>(property, "value", $"DateTime64({precision})");

        var value = new DateTimeOffset(Sample, TimeSpan.Zero);
        var rows = new List<DateTimeOffsetRow> { new() { Value = value } };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        extractor.ExtractAndWrite(ref writer, rows, 1);

        Assert.Equal(8, buffer.WrittenCount);
        Assert.Equal(ExpectedWire(Sample, precision), BitConverter.ToInt64(buffer.WrittenSpan));
    }

    [Theory]
    [InlineData(3)]
    [InlineData(9)]
    public void NullableDateTimeOffset_DateTime64_WritesExactInt64(int precision)
    {
        var property = typeof(NullableDateTimeOffsetRow).GetProperty(nameof(NullableDateTimeOffsetRow.Value))!;
        var extractor = ColumnExtractorFactory.Create<NullableDateTimeOffsetRow>(property, "value", $"Nullable(DateTime64({precision}))");

        var value = new DateTimeOffset(Sample, TimeSpan.Zero);
        var rows = new List<NullableDateTimeOffsetRow>
        {
            new() { Value = value },
            new() { Value = null },
        };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        extractor.ExtractAndWrite(ref writer, rows, 2);

        var span = buffer.WrittenSpan;
        Assert.Equal(2 + 2 * 8, span.Length);
        Assert.Equal(0x00, span[0]);
        Assert.Equal(0x01, span[1]);
        Assert.Equal(ExpectedWire(Sample, precision), BitConverter.ToInt64(span[2..10]));
        Assert.Equal(0L, BitConverter.ToInt64(span[10..18]));
    }

    [Fact]
    public void DateTime64_Precision9_RegressionTest_DoesNotLoseNanosecondDigits()
    {
        // This is the regression guard for the double-precision bulk-insert bug:
        // totalSeconds ≈ 1.7e9, × 1e9 ≈ 1.7e18 which exceeds double's ~15.95 sig digits
        // and used to silently drop the last 2-3 nanosecond digits.
        var property = typeof(DateTimeRow).GetProperty(nameof(DateTimeRow.Value))!;
        var extractor = ColumnExtractorFactory.Create<DateTimeRow>(property, "value", "DateTime64(9)");

        var rows = new List<DateTimeRow> { new() { Value = Sample } };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        extractor.ExtractAndWrite(ref writer, rows, 1);

        var actual = BitConverter.ToInt64(buffer.WrittenSpan);
        var expected = (Sample - UnixEpoch).Ticks * 100L;
        Assert.Equal(expected, actual);
    }

    private static long ExpectedWire(DateTime value, int precision)
    {
        var utc = value.Kind == DateTimeKind.Utc ? value : value.ToUniversalTime();
        var ticks = (utc - UnixEpoch).Ticks;
        if (precision > 7)
        {
            return ticks * (long)Math.Pow(10, precision - 7);
        }
        var ticksPerUnit = TimeSpan.TicksPerSecond / (long)Math.Pow(10, precision);
        return ticks / ticksPerUnit;
    }

    private class DateTimeRow
    {
        public DateTime Value { get; set; }
    }

    private class NullableDateTimeRow
    {
        public DateTime? Value { get; set; }
    }

    private class DateTimeOffsetRow
    {
        public DateTimeOffset Value { get; set; }
    }

    private class NullableDateTimeOffsetRow
    {
        public DateTimeOffset? Value { get; set; }
    }
}
