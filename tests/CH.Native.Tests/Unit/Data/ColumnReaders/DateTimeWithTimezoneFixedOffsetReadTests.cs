using System.Buffers;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// Complements <c>FixedOffsetTimeZoneParseTests</c>: verifies the reader actually applies a synthetic
/// <c>Fixed/UTC±HH:MM</c> offset when materializing a value (driver #370), producing a
/// <see cref="DateTimeOffset"/> with the right instant and offset.
/// </summary>
public class DateTimeWithTimezoneFixedOffsetReadTests
{
    private static DateTimeOffset ReadEpoch(string timezone)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt32(0); // Unix epoch

        var columnReader = new DateTimeWithTimezoneColumnReader(timezone);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        return columnReader.ReadValue(ref reader);
    }

    [Fact]
    public void Read_FixedPositiveOffset_AppliesOffsetToInstant()
    {
        var result = ReadEpoch("Fixed/UTC+05:30");

        Assert.Equal(TimeSpan.FromMinutes(330), result.Offset);
        Assert.Equal(DateTimeOffset.UnixEpoch, result.ToUniversalTime());
    }

    [Fact]
    public void Read_FixedNegativeOffset_AppliesOffsetToInstant()
    {
        var result = ReadEpoch("UTC-08:00");

        Assert.Equal(TimeSpan.FromHours(-8), result.Offset);
        Assert.Equal(DateTimeOffset.UnixEpoch, result.ToUniversalTime());
    }
}
