using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class Time64ColumnReaderTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(6)]
    [InlineData(9)]
    public void TypeName_ReflectsPrecision(int precision)
    {
        Assert.Equal($"Time64({precision})", new Time64ColumnReader(precision).TypeName);
    }

    [Fact]
    public void ClrType_IsTimeOnly()
    {
        Assert.Equal(typeof(TimeOnly), new Time64ColumnReader(3).ClrType);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(10)]
    public void Constructor_PrecisionOutOfRange_Throws(int precision)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new Time64ColumnReader(precision));
    }

    [Fact]
    public void ReadValue_Precision3_DecodesMilliseconds()
    {
        // 1h2m3.456s = 3723456 ms
        long value = 3_723_456L;
        var bytes = new byte[8];
        BitConverter.TryWriteBytes(bytes, value);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var result = new Time64ColumnReader(3).ReadValue(ref reader);

        Assert.Equal(new TimeOnly(1, 2, 3, 456), result);
    }

    [Fact]
    public void ReadValue_Precision0_DecodesSeconds()
    {
        long value = 3661L; // 1:01:01
        var bytes = new byte[8];
        BitConverter.TryWriteBytes(bytes, value);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var result = new Time64ColumnReader(0).ReadValue(ref reader);

        Assert.Equal(new TimeOnly(1, 1, 1), result);
    }

    [Fact]
    public void ReadValue_Precision6_DecodesMicroseconds()
    {
        // 1 second + 123456 microseconds = 1_123_456 us
        long value = 1_123_456L;
        var bytes = new byte[8];
        BitConverter.TryWriteBytes(bytes, value);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var result = new Time64ColumnReader(6).ReadValue(ref reader);

        // 1 second + 123456 us = 1234560 ticks (since 1 us = 10 ticks)
        var expected = new TimeOnly(TimeSpan.FromSeconds(1).Ticks + 1234560);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void ReadValue_Precision9_TruncatesToTickPrecision()
    {
        // 1 second + 123_456_789 nanoseconds.
        // .NET ticks are 100ns, so 123_456_789 ns truncates to 1_234_567 ticks.
        long value = 1_000_000_000L + 123_456_789L;
        var bytes = new byte[8];
        BitConverter.TryWriteBytes(bytes, value);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var result = new Time64ColumnReader(9).ReadValue(ref reader);

        var expectedTicks = TimeSpan.TicksPerSecond + 1_234_567L;
        Assert.Equal(expectedTicks, result.Ticks);
    }

    [Theory]
    [InlineData(3, -1L)]
    [InlineData(3, 86_400_000L)] // 24h in ms — out of range
    [InlineData(0, 86_400L)]
    public void ReadValue_OutOfRange_Throws(int precision, long value)
    {
        var bytes = new byte[8];
        BitConverter.TryWriteBytes(bytes, value);
        var columnReader = new Time64ColumnReader(precision);

        Assert.Throws<OverflowException>(() => ReadOne(columnReader, bytes));

        static void ReadOne(Time64ColumnReader r, byte[] data)
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(data));
            r.ReadValue(ref pr);
        }
    }

    [Fact]
    public void Registry_ResolvesParameterisedTime64()
    {
        var reader = ColumnReaderRegistry.Default.GetReader("Time64(3)");

        var typed = Assert.IsType<Time64ColumnReader>(reader);
        Assert.Equal(3, typed.Precision);
    }
}
