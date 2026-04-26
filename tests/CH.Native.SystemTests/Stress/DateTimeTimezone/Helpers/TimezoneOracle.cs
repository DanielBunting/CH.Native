namespace CH.Native.SystemTests.Stress.DateTimeTimezone.Helpers;

/// <summary>
/// Client-side oracle: what the library *should* compute for a given UTC instant in a given
/// IANA zone, derived directly from <see cref="TimeZoneInfo"/>. Tests assert that ClickHouse's
/// server-side rendering and the library's reader output both agree with this oracle.
/// </summary>
internal static class TimezoneOracle
{
    /// <summary>
    /// Converts a UTC instant to the local wall-clock time of the named zone.
    /// </summary>
    public static DateTime ToLocal(DateTime utc, string ianaId)
    {
        if (utc.Kind != DateTimeKind.Utc)
            throw new ArgumentException("Instant must be Kind=Utc", nameof(utc));

        var tz = TimeZoneInfo.FindSystemTimeZoneById(ianaId);
        return TimeZoneInfo.ConvertTimeFromUtc(utc, tz);
    }

    /// <summary>
    /// Returns the offset of the named zone at the given UTC instant.
    /// </summary>
    public static TimeSpan OffsetAt(DateTime utc, string ianaId)
    {
        var tz = TimeZoneInfo.FindSystemTimeZoneById(ianaId);
        return tz.GetUtcOffset(utc);
    }

    /// <summary>
    /// Renders a UTC instant in the named zone using ClickHouse's <c>formatDateTime</c>
    /// token syntax. Supports the subset: <c>%Y</c>, <c>%m</c> (numeric month),
    /// <c>%d</c>, <c>%H</c>, <c>%i</c> (minute), <c>%S</c>. Note that ClickHouse uses
    /// <c>%i</c> for minute (not <c>%M</c>, which is the full month name).
    /// </summary>
    public static string LocalRendering(DateTime utc, string ianaId, string clickhouseFormat)
    {
        var local = ToLocal(utc, ianaId);
        var dotnetFormat = ClickhouseFormatToDotnet(clickhouseFormat);
        return local.ToString(dotnetFormat, System.Globalization.CultureInfo.InvariantCulture);
    }

    private static string ClickhouseFormatToDotnet(string clickhouseFormat)
    {
        return clickhouseFormat
            .Replace("%Y", "yyyy")
            .Replace("%m", "MM")
            .Replace("%d", "dd")
            .Replace("%H", "HH")
            .Replace("%i", "mm")
            .Replace("%S", "ss");
    }
}
