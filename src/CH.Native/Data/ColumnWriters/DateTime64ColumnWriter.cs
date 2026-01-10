using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for DateTime64 values.
/// DateTime64 stores high-precision timestamps as Int64 with configurable precision (0-9).
/// </summary>
public sealed class DateTime64ColumnWriter : IColumnWriter<DateTime>
{
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private readonly int _precision;
    private readonly string? _timezone;
    private readonly long _ticksPerUnit;

    /// <summary>
    /// Creates a DateTime64 writer with the specified precision and optional timezone.
    /// </summary>
    /// <param name="precision">The precision (0-9). 0=seconds, 3=milliseconds, 6=microseconds, 9=nanoseconds.</param>
    /// <param name="timezone">Optional timezone name (stored for type name but not applied).</param>
    public DateTime64ColumnWriter(int precision, string? timezone = null)
    {
        if (precision < 0 || precision > 9)
            throw new ArgumentOutOfRangeException(nameof(precision), "Precision must be between 0 and 9.");

        _precision = precision;
        _timezone = timezone;

        // Calculate ticks per unit based on precision
        var divisor = (long)Math.Pow(10, precision);
        _ticksPerUnit = TimeSpan.TicksPerSecond / divisor;
    }

    /// <inheritdoc />
    public string TypeName => _timezone != null
        ? $"DateTime64({_precision}, '{_timezone}')"
        : $"DateTime64({_precision})";

    /// <inheritdoc />
    public Type ClrType => typeof(DateTime);

    /// <summary>
    /// Gets the precision (number of decimal places in seconds).
    /// </summary>
    public int Precision => _precision;

    /// <summary>
    /// Gets the timezone name (if specified).
    /// </summary>
    public string? Timezone => _timezone;

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, DateTime[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, DateTime value)
    {
        var utcValue = value.Kind == DateTimeKind.Local ? value.ToUniversalTime() : value;
        var ticks = (utcValue - UnixEpoch).Ticks;

        long result;
        if (_precision > 7)
        {
            // For precision 8 or 9, we need to multiply by the additional factor
            var multiplier = (long)Math.Pow(10, _precision - 7);
            result = ticks * multiplier;
        }
        else
        {
            result = ticks / _ticksPerUnit;
        }

        writer.WriteInt64(result);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, (DateTime)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        WriteValue(ref writer, (DateTime)value!);
    }
}
