using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

// DateTime64(8/9) block reads store the raw Int64 wire values (DateTime64RawColumn) so
// the sub-tick digits stay reachable via GetRawValue / GetFieldValue<long>, while
// GetValue keeps the long-standing truncated-DateTime default. Precision <= 7 is
// tick-exact and must keep TypedColumn<DateTime> (the typed fast path).
public class DateTime64RawColumnTests
{
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static ITypedColumn ReadColumn(int precision, params long[] rawValues)
    {
        var bytes = new byte[rawValues.Length * 8];
        for (int i = 0; i < rawValues.Length; i++)
        {
            BitConverter.TryWriteBytes(bytes.AsSpan(i * 8), rawValues[i]);
        }
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        IColumnReader columnReader = new DateTime64ColumnReader(precision, "UTC");
        return columnReader.ReadTypedColumn(ref reader, rawValues.Length);
    }

    [Theory]
    [InlineData(8)]
    [InlineData(9)]
    public void HighPrecision_BlockRead_ReturnsRawColumn(int precision)
    {
        using var column = ReadColumn(precision, 123L);

        var raw = Assert.IsType<DateTime64RawColumn>(column);
        Assert.Equal(precision, raw.Precision);
        Assert.Equal("UTC", raw.Timezone);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(6)]
    [InlineData(7)]
    public void TickExactPrecision_BlockRead_KeepsTypedDateTimeColumn(int precision)
    {
        using var column = ReadColumn(precision, 123L);

        Assert.IsType<TypedColumn<DateTime>>(column);
    }

    [Fact]
    public void Precision9_GetRawValue_ExactNanoseconds_GetValueTruncated()
    {
        const long nanos = 1_704_067_200_123_456_789; // 2024-01-01 00:00:00.123456789 UTC
        using var column = (DateTime64RawColumn)ReadColumn(9, nanos);

        Assert.Equal(nanos, column.GetRawValue(0));

        // String/DateTime view: truncated toward zero to 100ns ticks — .1234567
        var expected = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(1234567);
        Assert.Equal(expected, column.GetValue(0));
    }

    [Fact]
    public void Precision8_GetRawValue_Exact()
    {
        const long units = 170_406_720_012_345_678; // 10ns units
        using var column = (DateTime64RawColumn)ReadColumn(8, units);

        Assert.Equal(units, column.GetRawValue(0));
        Assert.Equal(UnixEpoch.AddTicks(units / 10), column.GetValue(0));
    }

    // Pre-epoch values divide toward zero — pins the long-standing truncation semantics.
    [Fact]
    public void Precision9_PreEpoch_TruncatesTowardZero()
    {
        using var column = (DateTime64RawColumn)ReadColumn(9, -150L);

        Assert.Equal(-150L, column.GetRawValue(0));
        Assert.Equal(UnixEpoch.AddTicks(-1), column.GetValue(0));
    }

    [Fact]
    public void MultiRow_RawAndTruncatedViewsAlign()
    {
        long[] raws = [0L, 99L, 100L, 1_000_000_001L];
        using var column = (DateTime64RawColumn)ReadColumn(9, raws);

        Assert.Equal(raws.Length, column.Count);
        for (int i = 0; i < raws.Length; i++)
        {
            Assert.Equal(raws[i], column.GetRawValue(i));
            Assert.Equal(UnixEpoch.AddTicks(raws[i] / 100), column.GetValue(i));
        }
    }

    [Fact]
    public void GetRawValue_OutOfRange_Throws()
    {
        using var column = (DateTime64RawColumn)ReadColumn(9, 1L);

        Assert.Throws<ArgumentOutOfRangeException>(() => column.GetRawValue(1));
        Assert.Throws<ArgumentOutOfRangeException>(() => column.GetRawValue(-1));
    }

    [Fact]
    public void GetRawValue_AfterDispose_Throws()
    {
        var column = (DateTime64RawColumn)ReadColumn(9, 1L);
        column.Dispose();

        Assert.Throws<ObjectDisposedException>(() => column.GetRawValue(0));
        Assert.Throws<ObjectDisposedException>(() => column.GetValue(0));
    }
}
