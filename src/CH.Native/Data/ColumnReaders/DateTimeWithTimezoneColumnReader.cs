using System.Buffers;
using System.Runtime.InteropServices;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for DateTime values with timezone parameter.
/// Returns DateTimeOffset for proper timezone semantics.
/// </summary>
/// <remarks>
/// Wire format is the same as DateTime (UInt32 Unix timestamp),
/// but values are interpreted in the specified timezone.
/// </remarks>
public sealed class DateTimeWithTimezoneColumnReader : IColumnReader<DateTimeOffset>
{
    private readonly TimeZoneInfo _timezone;
    private readonly string _originalTimezone;

    /// <summary>
    /// Creates a DateTime reader with the specified timezone.
    /// </summary>
    /// <param name="timezone">The IANA or Windows timezone name (e.g., "UTC", "America/New_York").</param>
    public DateTimeWithTimezoneColumnReader(string timezone)
    {
        _originalTimezone = timezone;
        _timezone = FindTimeZone(timezone);
    }

    /// <inheritdoc />
    public string TypeName => $"DateTime('{_originalTimezone}')";

    /// <inheritdoc />
    public Type ClrType => typeof(DateTimeOffset);

    /// <summary>
    /// Gets the timezone used for conversion.
    /// </summary>
    public TimeZoneInfo Timezone => _timezone;

    /// <inheritdoc />
    public DateTimeOffset ReadValue(ref ProtocolReader reader)
    {
        var timestamp = reader.ReadUInt32();
        var utcDateTime = DateTimeOffset.UnixEpoch.AddSeconds(timestamp);
        return TimeZoneInfo.ConvertTime(utcDateTime, _timezone);
    }

    /// <inheritdoc />
    public TypedColumn<DateTimeOffset> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<DateTimeOffset>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = ReadValue(ref reader);
        }
        return new TypedColumn<DateTimeOffset>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }

    /// <summary>
    /// Finds a TimeZoneInfo by IANA or Windows timezone ID.
    /// </summary>
    private static TimeZoneInfo FindTimeZone(string timezone)
    {
        // Handle UTC specially as it's the most common case
        if (timezone.Equals("UTC", StringComparison.OrdinalIgnoreCase))
            return TimeZoneInfo.Utc;

        // Try direct lookup first (works on the native platform)
        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById(timezone);
        }
        catch (TimeZoneNotFoundException)
        {
            // On Windows, we may need to convert IANA to Windows ID
            // On Linux/macOS, IANA IDs work directly
        }

        // Try converting IANA to Windows timezone if on Windows
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            var windowsId = TryConvertIanaToWindows(timezone);
            if (windowsId != null)
            {
                try
                {
                    return TimeZoneInfo.FindSystemTimeZoneById(windowsId);
                }
                catch (TimeZoneNotFoundException)
                {
                    // Fall through to exception
                }
            }
        }

        throw new TimeZoneNotFoundException($"Timezone '{timezone}' not found.");
    }

    /// <summary>
    /// Attempts to convert common IANA timezone IDs to Windows timezone IDs.
    /// </summary>
    private static string? TryConvertIanaToWindows(string ianaId)
    {
        // Common IANA to Windows timezone mappings
        return ianaId switch
        {
            "America/New_York" => "Eastern Standard Time",
            "America/Chicago" => "Central Standard Time",
            "America/Denver" => "Mountain Standard Time",
            "America/Los_Angeles" => "Pacific Standard Time",
            "America/Phoenix" => "US Mountain Standard Time",
            "America/Anchorage" => "Alaskan Standard Time",
            "Pacific/Honolulu" => "Hawaiian Standard Time",
            "Europe/London" => "GMT Standard Time",
            "Europe/Paris" => "Romance Standard Time",
            "Europe/Berlin" => "W. Europe Standard Time",
            "Europe/Moscow" => "Russian Standard Time",
            "Asia/Tokyo" => "Tokyo Standard Time",
            "Asia/Shanghai" => "China Standard Time",
            "Asia/Singapore" => "Singapore Standard Time",
            "Asia/Dubai" => "Arabian Standard Time",
            "Asia/Kolkata" => "India Standard Time",
            "Australia/Sydney" => "AUS Eastern Standard Time",
            "Australia/Perth" => "W. Australia Standard Time",
            "Pacific/Auckland" => "New Zealand Standard Time",
            _ => null
        };
    }
}
