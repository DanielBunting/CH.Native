using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// DateTime('Tz') has the same wire format as DateTime (UInt32 Unix seconds),
/// but the reader applies the timezone via TimeZoneInfo.ConvertTime to produce
/// a DateTimeOffset whose offset reflects the configured zone.
/// </summary>
public class DateTimeWithTimezoneColumnReaderTests
{
    [Fact]
    public void TypeName_EmbedsTimezone()
    {
        Assert.Equal("DateTime('UTC')", new DateTimeWithTimezoneColumnReader("UTC").TypeName);
    }

    [Fact]
    public void ClrType_IsDateTimeOffset()
    {
        Assert.Equal(typeof(DateTimeOffset), new DateTimeWithTimezoneColumnReader("UTC").ClrType);
    }

    [Fact]
    public void ReadValue_Utc_ReturnsUtcOffset()
    {
        var wire = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(wire, 0); // epoch
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(wire));

        var result = new DateTimeWithTimezoneColumnReader("UTC").ReadValue(ref reader);

        Assert.Equal(DateTimeOffset.UnixEpoch, result);
        Assert.Equal(TimeSpan.Zero, result.Offset);
    }

    [Fact]
    public void ReadValue_KnownTimestamp_DecodesToCorrectInstant()
    {
        // 1704067200 = 2024-01-01 00:00:00 UTC
        var wire = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(wire, 1704067200);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(wire));

        var result = new DateTimeWithTimezoneColumnReader("UTC").ReadValue(ref reader);

        Assert.Equal(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero), result);
    }

    [Fact]
    public void Constructor_UnknownTimezone_Throws()
    {
        Assert.Throws<TimeZoneNotFoundException>(() =>
            new DateTimeWithTimezoneColumnReader("Not/A/RealZone"));
    }

    [Fact]
    public void Timezone_ExposesResolvedZone()
    {
        var sut = new DateTimeWithTimezoneColumnReader("UTC");
        Assert.Equal(TimeZoneInfo.Utc, sut.Timezone);
    }
}
