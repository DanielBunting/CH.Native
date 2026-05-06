using CH.Native.Exceptions;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Time64 values.
/// Time64 stores high-precision time-of-day as Int64 sub-seconds since 00:00:00 with configurable precision (0-9).
/// </summary>
internal sealed class Time64ColumnWriter : IColumnWriter<TimeOnly>
{
    private readonly int _precision;
    private readonly long _ticksPerUnit;

    /// <summary>
    /// Creates a Time64 writer with the specified precision.
    /// </summary>
    /// <param name="precision">The precision (0-9). 0=seconds, 3=milliseconds, 6=microseconds, 9=nanoseconds.</param>
    public Time64ColumnWriter(int precision)
    {
        if (precision < 0 || precision > 9)
            throw new ArgumentOutOfRangeException(nameof(precision), "Precision must be between 0 and 9.");

        _precision = precision;
        var unitsPerSecond = (long)Math.Pow(10, precision);
        _ticksPerUnit = precision <= 7 ? TimeSpan.TicksPerSecond / unitsPerSecond : 0;
    }

    /// <inheritdoc />
    public string TypeName => $"Time64({_precision})";

    /// <inheritdoc />
    public Type ClrType => typeof(TimeOnly);

    /// <summary>
    /// Gets the precision (number of decimal places in seconds).
    /// </summary>
    public int Precision => _precision;

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, TimeOnly[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, TimeOnly value)
    {
        var ticks = value.Ticks;

        long result;
        if (_precision > 7)
        {
            // TimeOnly is bounded to one day so this never overflows in
            // practice — but the check is defense-in-depth and matches the
            // sibling DateTime64 writer's contract.
            var multiplier = (long)Math.Pow(10, _precision - 7);
            try
            {
                result = checked(ticks * multiplier);
            }
            catch (OverflowException ex)
            {
                throw new ClickHouseProtocolException(
                    $"Time64({_precision}) value {value} produces a tick-count that overflows Int64.", ex);
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
            WriteValue(ref writer, (TimeOnly)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        WriteValue(ref writer, (TimeOnly)value!);
    }
}
