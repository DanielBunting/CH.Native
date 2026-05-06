using CH.Native.Exceptions;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for DateTime64 values.
/// DateTime64 stores high-precision timestamps as Int64 with configurable precision (0-9).
/// </summary>
internal sealed class DateTime64ColumnWriter : IColumnWriter<DateTime>
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

        // For precision > 7, one wire unit is smaller than a tick, so no integer
        // ticks-per-unit exists — leave it zero and let WriteValue take the multiply branch.
        var divisor = (long)Math.Pow(10, precision);
        _ticksPerUnit = precision <= 7 ? TimeSpan.TicksPerSecond / divisor : 0;
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
    /// <remarks>
    /// <see cref="DateTimeKind"/> handling:
    /// <list type="bullet">
    ///   <item><description><c>Utc</c> — written verbatim.</description></item>
    ///   <item><description><c>Local</c> — converted to UTC before encoding.</description></item>
    ///   <item><description><c>Unspecified</c> — treated as already-UTC. <b>Silent</b>:
    ///     legacy code, JSON deserializers, or EF projections that hand back an
    ///     unspecified-kind DateTime representing local time will be inserted at
    ///     the wrong instant by the local UTC offset. Callers uncertain about
    ///     kind should normalise via <c>DateTime.SpecifyKind(value, DateTimeKind.Utc)</c>
    ///     (if the value really is UTC) or <c>DateTime.SpecifyKind(value, DateTimeKind.Local).ToUniversalTime()</c>
    ///     (if it represents local wall-clock time) before binding.</description></item>
    /// </list>
    /// </remarks>
    public void WriteValue(ref ProtocolWriter writer, DateTime value)
    {
        var utcValue = value.Kind == DateTimeKind.Local ? value.ToUniversalTime() : value;
        var ticks = (utcValue - UnixEpoch).Ticks;

        long result;
        if (_precision > 7)
        {
            // Precision 8 / 9: multiply ticks by the sub-tick factor. DateTime
            // values near .NET's MaxValue can produce ticks counts large enough
            // that the multiplication overflows Int64; pre-fix this wrapped
            // silently to a negative wire value. checked() surfaces the
            // overflow and we wrap it in a typed protocol exception so it
            // doesn't get classified as transient downstream.
            var multiplier = (long)Math.Pow(10, _precision - 7);
            try
            {
                result = checked(ticks * multiplier);
            }
            catch (OverflowException ex)
            {
                throw new ClickHouseProtocolException(
                    $"DateTime64({_precision}) value {value:O} produces a tick-count that overflows Int64; " +
                    "ClickHouse cannot represent this moment at the requested precision.", ex);
            }
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
