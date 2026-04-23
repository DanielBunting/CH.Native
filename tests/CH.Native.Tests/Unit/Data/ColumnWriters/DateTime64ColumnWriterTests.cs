using System.Buffers;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class DateTime64ColumnWriterTests
{
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(6)]
    [InlineData(8)]
    [InlineData(9)]
    public void TypeName_ReflectsPrecision(int precision)
    {
        Assert.Equal($"DateTime64({precision})", new DateTime64ColumnWriter(precision).TypeName);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(10)]
    public void Constructor_PrecisionOutOfRange_Throws(int precision)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new DateTime64ColumnWriter(precision));
    }

    [Fact]
    public void WriteValue_Precision3_EncodesMilliseconds()
    {
        var value = new DateTime(2024, 1, 15, 12, 30, 45, 123, DateTimeKind.Utc);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new DateTime64ColumnWriter(3).WriteValue(ref writer, value);

        var expected = (long)(value - UnixEpoch).TotalMilliseconds;
        Assert.Equal(expected, BitConverter.ToInt64(buffer.WrittenSpan));
    }

    [Fact]
    public void WriteValue_Precision9_PreservesNanosecondTickResolution()
    {
        // 100-ns-tick-aligned nanosecond value so round-trip is exact.
        var value = UnixEpoch.AddTicks(
            (new DateTime(2024, 1, 15, 12, 30, 45, 0, DateTimeKind.Utc) - UnixEpoch).Ticks + 1_234_567);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new DateTime64ColumnWriter(9).WriteValue(ref writer, value);

        var ticks = (value - UnixEpoch).Ticks;
        Assert.Equal(ticks * 100L, BitConverter.ToInt64(buffer.WrittenSpan));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    public void WriteValue_RoundTripsThroughReader(int precision)
    {
        // Tick-aligned source so nothing is lost for precision ≥ 7.
        var baseSeconds = (long)(new DateTime(2024, 1, 15, 12, 30, 45, 0, DateTimeKind.Utc) - UnixEpoch).TotalSeconds;
        var original = UnixEpoch.AddTicks(baseSeconds * TimeSpan.TicksPerSecond + 1_234_567);

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new DateTime64ColumnWriter(precision).WriteValue(ref writer, original);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = new DateTime64ColumnReader(precision).ReadValue(ref reader);

        // Low-precision round-trips floor to the precision's unit.
        DateTime expected;
        if (precision >= 7)
        {
            expected = original;
        }
        else
        {
            var ticksPerUnit = TimeSpan.TicksPerSecond / (long)Math.Pow(10, precision);
            expected = UnixEpoch.AddTicks((original - UnixEpoch).Ticks / ticksPerUnit * ticksPerUnit);
        }
        Assert.Equal(expected, result);
    }
}
