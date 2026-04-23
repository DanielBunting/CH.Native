using System.Buffers;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class DateTime64ColumnReaderTests
{
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    public void TypeName_ReflectsPrecision(int precision)
    {
        Assert.Equal($"DateTime64({precision})", new DateTime64ColumnReader(precision).TypeName);
    }

    [Fact]
    public void TypeName_IncludesTimezone()
    {
        Assert.Equal("DateTime64(3, 'UTC')", new DateTime64ColumnReader(3, "UTC").TypeName);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(10)]
    public void Constructor_PrecisionOutOfRange_Throws(int precision)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new DateTime64ColumnReader(precision));
    }

    [Fact]
    public void ReadValue_Precision3_DecodesMilliseconds()
    {
        // 2024-01-15 12:30:45.123 UTC
        var target = new DateTime(2024, 1, 15, 12, 30, 45, 123, DateTimeKind.Utc);
        long wire = (long)(target - UnixEpoch).TotalMilliseconds;

        var result = ReadOne(new DateTime64ColumnReader(3), wire);

        Assert.Equal(target, result);
    }

    [Fact]
    public void ReadValue_Precision6_DecodesMicroseconds()
    {
        // 2024-01-15 12:30:45.123456 UTC
        var baseTicks = (new DateTime(2024, 1, 15, 12, 30, 45, 0, DateTimeKind.Utc) - UnixEpoch).Ticks;
        long wire = (baseTicks / TimeSpan.TicksPerMillisecond) * 1000 + 123_456;

        var result = ReadOne(new DateTime64ColumnReader(6), wire);

        var expected = UnixEpoch.AddTicks(baseTicks + 1234_56 * 10); // 123456 us = 1234560 ticks
        Assert.Equal(expected, result);
    }

    [Fact]
    public void ReadValue_Precision8_DoesNotCollapseToEpoch()
    {
        // 2024-01-15 12:30:45 UTC + 12345678 × 10ns = +123_456_780 ns
        var baseSeconds = (long)(new DateTime(2024, 1, 15, 12, 30, 45, 0, DateTimeKind.Utc) - UnixEpoch).TotalSeconds;
        long wire = baseSeconds * 100_000_000L + 12_345_678L;

        var result = ReadOne(new DateTime64ColumnReader(8), wire);

        // 10ns units → /10 gives ticks (100ns).
        var expected = UnixEpoch.AddTicks(baseSeconds * TimeSpan.TicksPerSecond + 1_234_567);
        Assert.Equal(expected, result);
        Assert.NotEqual(UnixEpoch, result);
    }

    [Fact]
    public void ReadValue_Precision9_TruncatesToTickPrecision()
    {
        // 2024-01-15 12:30:45 UTC + 123_456_789 ns
        var baseSeconds = (long)(new DateTime(2024, 1, 15, 12, 30, 45, 0, DateTimeKind.Utc) - UnixEpoch).TotalSeconds;
        long wire = baseSeconds * 1_000_000_000L + 123_456_789L;

        var result = ReadOne(new DateTime64ColumnReader(9), wire);

        // 1ns units → /100 gives ticks; 89 ns below tick resolution is truncated.
        var expected = UnixEpoch.AddTicks(baseSeconds * TimeSpan.TicksPerSecond + 1_234_567);
        Assert.Equal(expected, result);
        Assert.NotEqual(UnixEpoch, result);
    }

    private static DateTime ReadOne(DateTime64ColumnReader reader, long wire)
    {
        var bytes = new byte[8];
        BitConverter.TryWriteBytes(bytes, wire);
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        return reader.ReadValue(ref pr);
    }
}
