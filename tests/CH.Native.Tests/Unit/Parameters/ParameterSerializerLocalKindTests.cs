using System.Globalization;
using CH.Native.Parameters;
using Xunit;

namespace CH.Native.Tests.Unit.Parameters;

/// <summary>
/// Pre-fix <see cref="ParameterSerializer.SerializeDateTimeRaw"/> formatted
/// the <see cref="DateTime"/> verbatim regardless of <see cref="DateTime.Kind"/>.
/// A <c>Local</c>-kinded value sent the local clock face to a UTC column,
/// landing as a moment offset by the local-zone delta. The corresponding
/// <see cref="CH.Native.Data.ColumnWriters.DateTime64ColumnWriter"/> already
/// converts <c>Local</c> → UTC; the parameter path now matches.
/// </summary>
public class ParameterSerializerLocalKindTests
{
    [Fact]
    public void DateTime64Param_LocalAndUtcOfSameMoment_ProduceSameWireString()
    {
        // Use the system-local zone consistently — DateTimeKind.Local is
        // anchored to TimeZoneInfo.Local, so any "expected UTC" must be
        // computed via the same zone.
        var localKind = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Local);
        var utcKind = localKind.ToUniversalTime();

        // Skip the assertion if the test machine happens to run in UTC — both
        // values are then equal so the test wouldn't differentiate the bug.
        if (TimeZoneInfo.Local.GetUtcOffset(localKind) == TimeSpan.Zero)
            return;

        var localWire = Serialize(localKind, "DateTime64(6)");
        var utcWire = Serialize(utcKind, "DateTime64(6)");

        Assert.Equal(utcWire, localWire);
    }

    [Fact]
    public void DateTimeParam_UtcKind_FormatsAsIs()
    {
        // Sanity: UTC values must not be re-converted by the fix.
        var dt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var wire = Serialize(dt, "DateTime");
        Assert.Equal("2024-01-01 00:00:00", wire);
    }

    private static string Serialize(DateTime dt, string clickHouseType)
    {
        // SerializeDateTimeRaw is private; route through the public Serialize
        // entry point, which delegates to it.
        var s = ParameterSerializer.Serialize(dt, clickHouseType);
        // Public Serialize wraps in single quotes for non-numeric types; strip
        // them for comparison.
        return s.Trim('\'');
    }

    private static string GetNonUtcTimezoneId()
    {
        // Linux test runners may not have Windows-style IDs; pick whatever the
        // OS exposes that has a non-zero offset.
        foreach (var tz in TimeZoneInfo.GetSystemTimeZones())
        {
            if (tz.BaseUtcOffset != TimeSpan.Zero) return tz.Id;
        }
        return TimeZoneInfo.Utc.Id;
    }
}
