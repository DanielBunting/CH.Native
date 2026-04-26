using System.Runtime.InteropServices;

namespace CH.Native.SystemTests.Stress.DateTimeTimezone.Helpers;

/// <summary>
/// Curated lists of IANA timezones and instants for parameterized timezone tests.
/// </summary>
internal static class TimezoneTestData
{
    /// <summary>
    /// IANA timezone IDs intentionally including standard, exotic, half-hour, quarter-hour,
    /// +14, and POSIX-inverted zones. Some are unsupported on Windows .NET and are skipped
    /// at row level by <see cref="IsSupportedOnHost"/>.
    /// </summary>
    public static readonly string[] CuratedZones =
    [
        "UTC",
        "America/New_York",
        "America/Chicago",
        "America/Los_Angeles",
        "Europe/London",
        "Europe/Berlin",
        "Europe/Moscow",
        "Asia/Tokyo",
        "Asia/Shanghai",
        "Asia/Kolkata",        // UTC+5:30
        "Asia/Kabul",          // UTC+4:30
        "Asia/Kathmandu",      // UTC+5:45
        "Asia/Tehran",
        "Australia/Sydney",
        "Australia/Lord_Howe", // 30-min DST
        "Pacific/Auckland",
        "Pacific/Chatham",     // UTC+12:45
        "Pacific/Kiritimati",  // UTC+14
        "Pacific/Marquesas",   // UTC-9:30
        "Etc/GMT+12",          // POSIX-inverted (= UTC-12)
    ];

    public static readonly DateTime[] CuratedInstants =
    [
        new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),
        new DateTime(2024, 3, 10, 9, 30, 0, DateTimeKind.Utc),    // US spring forward day
        new DateTime(2024, 11, 3, 6, 30, 0, DateTimeKind.Utc),    // US fall back day
        new DateTime(2038, 1, 19, 3, 14, 7, DateTimeKind.Utc),    // UInt32 boundary
        new DateTime(2099, 12, 31, 23, 59, 59, DateTimeKind.Utc),
    ];

    /// <summary>
    /// Cross-product of zones and instants for [Theory] [MemberData].
    /// Rows for zones not supported on the host platform are emitted with a Skip
    /// reason so the runner reports them as skipped, never as silent passes.
    /// </summary>
    public static IEnumerable<object[]> MatrixCases()
    {
        foreach (var zone in CuratedZones)
        {
            var supported = IsSupportedOnHost(zone);
            foreach (var instant in CuratedInstants)
            {
                yield return [zone, instant, supported];
            }
        }
    }

    public static IEnumerable<object[]> ZoneCases()
    {
        foreach (var zone in CuratedZones)
            yield return [zone, IsSupportedOnHost(zone)];
    }

    /// <summary>
    /// True if <see cref="TimeZoneInfo.FindSystemTimeZoneById"/> resolves the zone on
    /// the current host. On Linux/macOS all IANA zones resolve via tzdata. On Windows
    /// only the ~20 zones that the library's IANA→Windows mapping covers will resolve;
    /// the rest are reported as skipped.
    /// </summary>
    public static bool IsSupportedOnHost(string ianaId)
    {
        try
        {
            _ = TimeZoneInfo.FindSystemTimeZoneById(ianaId);
            return true;
        }
        catch (TimeZoneNotFoundException)
        {
            return false;
        }
        catch (InvalidTimeZoneException)
        {
            return false;
        }
    }

    public static bool IsWindows => RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
}
