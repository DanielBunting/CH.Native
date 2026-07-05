using CH.Native.Data.ColumnReaders;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// Covers <see cref="DateTimeWithTimezoneColumnReader.TryParseFixedOffsetTimeZone"/>, which resolves the
/// synthetic <c>Fixed/UTC±HH:MM</c> zone names ClickHouse can emit (driver #370) instead of throwing.
/// </summary>
public class FixedOffsetTimeZoneParseTests
{
    [Theory]
    [InlineData("Fixed/UTC+05:30", 5, 30)]
    [InlineData("UTC+05:30", 5, 30)]
    [InlineData("UTC-08:00", -8, 0)]
    [InlineData("GMT+01:00", 1, 0)]
    [InlineData("+05:45", 5, 45)]
    [InlineData("-03:00", -3, 0)]
    [InlineData("Fixed/UTC+14:00", 14, 0)]
    public void TryParse_ValidFixedOffsets(string name, int hours, int minutes)
    {
        var tz = DateTimeWithTimezoneColumnReader.TryParseFixedOffsetTimeZone(name);
        Assert.NotNull(tz);
        var expected = new TimeSpan(hours, hours < 0 ? -minutes : minutes, 0);
        Assert.Equal(expected, tz!.BaseUtcOffset);
    }

    [Theory]
    [InlineData("UTC")]
    [InlineData("Fixed/UTC+00:00")]
    [InlineData("GMT")]
    public void TryParse_ZeroOffset_IsUtc(string name)
    {
        var tz = DateTimeWithTimezoneColumnReader.TryParseFixedOffsetTimeZone(name);
        Assert.Equal(TimeZoneInfo.Utc, tz);
    }

    [Theory]
    [InlineData("America/New_York")]  // real IANA zone — must fall through to system lookup
    [InlineData("Europe/London")]
    [InlineData("UTC+15:00")]         // beyond ±14:00
    [InlineData("UTC+aa:bb")]
    [InlineData("")]
    [InlineData("   ")]
    public void TryParse_NonFixedOrOutOfRange_ReturnsNull(string name) =>
        Assert.Null(DateTimeWithTimezoneColumnReader.TryParseFixedOffsetTimeZone(name));
}
