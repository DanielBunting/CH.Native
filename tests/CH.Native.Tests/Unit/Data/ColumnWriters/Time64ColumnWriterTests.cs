using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class Time64ColumnWriterTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(6)]
    [InlineData(9)]
    public void TypeName_ReflectsPrecision(int precision)
    {
        Assert.Equal($"Time64({precision})", new Time64ColumnWriter(precision).TypeName);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(10)]
    public void Constructor_PrecisionOutOfRange_Throws(int precision)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new Time64ColumnWriter(precision));
    }

    [Fact]
    public void WriteValue_Precision3_EncodesMilliseconds()
    {
        var time = new TimeOnly(1, 2, 3, 456);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Time64ColumnWriter(3).WriteValue(ref writer, time);

        Assert.Equal(8, buffer.WrittenCount);
        Assert.Equal(3_723_456L, BitConverter.ToInt64(buffer.WrittenSpan));
    }

    [Fact]
    public void WriteValue_Precision0_EncodesSeconds()
    {
        var time = new TimeOnly(1, 1, 1);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Time64ColumnWriter(0).WriteValue(ref writer, time);

        Assert.Equal(3661L, BitConverter.ToInt64(buffer.WrittenSpan));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(6)]
    [InlineData(7)]
    public void WriteValue_RoundTripsThroughReader(int precision)
    {
        var original = new TimeOnly(13, 37, 42, 123);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Time64ColumnWriter(precision).WriteValue(ref writer, original);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = new Time64ColumnReader(precision).ReadValue(ref reader);

        // For precision < 3 the millisecond portion is lost — compare floor(value to that precision)
        var unitsPerSecond = (long)Math.Pow(10, precision);
        var ticksPerUnit = TimeSpan.TicksPerSecond / unitsPerSecond;
        var expected = new TimeOnly((original.Ticks / ticksPerUnit) * ticksPerUnit);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Registry_ResolvesParameterisedTime64()
    {
        var writer = ColumnWriterRegistry.Default.GetWriter("Time64(6)");

        var typed = Assert.IsType<Time64ColumnWriter>(writer);
        Assert.Equal(6, typed.Precision);
    }
}
