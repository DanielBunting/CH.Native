using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class IntervalColumnReaderTests
{
    public static IEnumerable<object[]> AllUnits() =>
        Enum.GetValues<IntervalUnit>().Select(u => new object[] { u });

    [Theory]
    [MemberData(nameof(AllUnits))]
    public void TypeName_MatchesServerTypeName(IntervalUnit unit)
    {
        Assert.Equal($"Interval{unit}", new IntervalColumnReader(unit).TypeName);
    }

    [Fact]
    public void ClrType_IsClickHouseInterval()
    {
        Assert.Equal(typeof(ClickHouseInterval), new IntervalColumnReader(IntervalUnit.Day).ClrType);
    }

    [Theory]
    [InlineData(3L)]
    [InlineData(0L)]
    [InlineData(-42L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void ReadValue_DecodesInt64LittleEndian(long value)
    {
        var bytes = new byte[8];
        BitConverter.TryWriteBytes(bytes, value);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var result = new IntervalColumnReader(IntervalUnit.Day).ReadValue(ref reader);

        Assert.Equal(new ClickHouseInterval(value, IntervalUnit.Day), result);
    }

    [Fact]
    public void ReadTypedColumn_ReadsAllRows()
    {
        long[] values = [1, -2, 3];
        var bytes = new byte[values.Length * 8];
        for (int i = 0; i < values.Length; i++)
        {
            BitConverter.TryWriteBytes(bytes.AsSpan(i * 8), values[i]);
        }
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = new IntervalColumnReader(IntervalUnit.Month).ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(3, column.Count);
        for (int i = 0; i < values.Length; i++)
        {
            Assert.Equal(new ClickHouseInterval(values[i], IntervalUnit.Month), column[i]);
        }
    }

    [Theory]
    [MemberData(nameof(AllUnits))]
    public void Registry_ResolvesAllIntervalTypes(IntervalUnit unit)
    {
        var reader = ColumnReaderRegistry.Default.GetReader($"Interval{unit}");
        Assert.IsType<IntervalColumnReader>(reader);
        Assert.Equal($"Interval{unit}", reader.TypeName);
    }

    // Nullable(IntervalX) composes through the existing NullableColumnReader since
    // ClickHouseInterval is a value type.
    [Fact]
    public void Registry_ResolvesNullableInterval_AndDecodes()
    {
        var reader = ColumnReaderRegistry.Default.GetReader("Nullable(IntervalDay)");

        // 2 rows: null mask [0,1] + two Int64 payloads (second is a null slot)
        var wire = new byte[2 + 16];
        wire[0] = 0;
        wire[1] = 1;
        BitConverter.TryWriteBytes(wire.AsSpan(2), 7L);
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(wire));

        using var column = reader.ReadTypedColumn(ref protocolReader, 2);

        Assert.Equal(new ClickHouseInterval(7, IntervalUnit.Day), column.GetValue(0));
        Assert.Null(column.GetValue(1));
    }
}

public class ClickHouseIntervalTests
{
    [Theory]
    [InlineData(IntervalUnit.Microsecond, 1_500_000L, 0, 0, 1, 500)]
    [InlineData(IntervalUnit.Millisecond, 1_500L, 0, 0, 1, 500)]
    [InlineData(IntervalUnit.Second, 90L, 0, 1, 30, 0)]
    [InlineData(IntervalUnit.Minute, 90L, 1, 30, 0, 0)]
    [InlineData(IntervalUnit.Hour, 25L, 25, 0, 0, 0)]
    public void ToTimeSpan_TimeUnits_ConvertExactly(IntervalUnit unit, long value, int h, int m, int s, int ms)
    {
        var ts = new ClickHouseInterval(value, unit).ToTimeSpan();
        Assert.Equal(new TimeSpan(0, h, m, s, ms), ts);
    }

    [Fact]
    public void ToTimeSpan_DayAndWeek()
    {
        Assert.Equal(TimeSpan.FromDays(3), new ClickHouseInterval(3, IntervalUnit.Day).ToTimeSpan());
        Assert.Equal(TimeSpan.FromDays(14), new ClickHouseInterval(2, IntervalUnit.Week).ToTimeSpan());
    }

    [Fact]
    public void ToTimeSpan_Nanoseconds_TruncateTowardZeroTo100nsTicks()
    {
        Assert.Equal(TimeSpan.FromTicks(1), new ClickHouseInterval(150, IntervalUnit.Nanosecond).ToTimeSpan());
        Assert.Equal(TimeSpan.FromTicks(-1), new ClickHouseInterval(-150, IntervalUnit.Nanosecond).ToTimeSpan());
        Assert.Equal(TimeSpan.Zero, new ClickHouseInterval(99, IntervalUnit.Nanosecond).ToTimeSpan());
    }

    [Theory]
    [InlineData(IntervalUnit.Month)]
    [InlineData(IntervalUnit.Quarter)]
    [InlineData(IntervalUnit.Year)]
    public void ToTimeSpan_CalendarUnits_Throw(IntervalUnit unit)
    {
        var interval = new ClickHouseInterval(1, unit);

        Assert.True(interval.IsCalendarUnit);
        Assert.Throws<NotSupportedException>(() => interval.ToTimeSpan());
    }

    [Fact]
    public void ToTimeSpan_Overflow_Throws()
    {
        Assert.Throws<OverflowException>(
            () => new ClickHouseInterval(long.MaxValue, IntervalUnit.Day).ToTimeSpan());
    }

    [Fact]
    public void Equality_And_ToString()
    {
        var a = new ClickHouseInterval(3, IntervalUnit.Day);
        var b = new ClickHouseInterval(3, IntervalUnit.Day);
        var c = new ClickHouseInterval(3, IntervalUnit.Hour);
        var d = new ClickHouseInterval(4, IntervalUnit.Day);

        Assert.True(a == b);
        Assert.True(a != c);
        Assert.True(a != d);
        Assert.False(a == d);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.Equal("3 Day", a.ToString());
    }

    [Fact]
    public void Equals_Object_RejectsNullAndOtherTypes()
    {
        var a = new ClickHouseInterval(3, IntervalUnit.Day);

        Assert.False(a.Equals(null));
        Assert.False(a.Equals("3 Day"));
        Assert.False(a.Equals(3L));
        Assert.True(a.Equals((object)new ClickHouseInterval(3, IntervalUnit.Day)));
    }

    [Theory]
    [InlineData(IntervalUnit.Nanosecond)]
    [InlineData(IntervalUnit.Microsecond)]
    [InlineData(IntervalUnit.Millisecond)]
    [InlineData(IntervalUnit.Second)]
    [InlineData(IntervalUnit.Minute)]
    [InlineData(IntervalUnit.Hour)]
    [InlineData(IntervalUnit.Day)]
    [InlineData(IntervalUnit.Week)]
    public void IsCalendarUnit_FalseForTimeUnits(IntervalUnit unit)
    {
        Assert.False(new ClickHouseInterval(1, unit).IsCalendarUnit);
    }
}
