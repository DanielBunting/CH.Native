using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Time64 values.
/// Time64 stores high-precision time-of-day as Int64 sub-seconds since 00:00:00 with configurable precision (0-9).
/// Always returns TimeOnly; values at precision 8/9 are truncated to 100ns ticks (mirrors DateTime64 behaviour).
/// </summary>
public sealed class Time64ColumnReader : IColumnReader<TimeOnly>
{
    private const long SecondsPerDay = 86_400;

    private readonly int _precision;
    private readonly long _unitsPerDay;
    private readonly long _ticksPerUnit;

    /// <summary>
    /// Creates a Time64 reader with the specified precision.
    /// </summary>
    /// <param name="precision">The precision (0-9). 0=seconds, 3=milliseconds, 6=microseconds, 9=nanoseconds.</param>
    public Time64ColumnReader(int precision)
    {
        if (precision < 0 || precision > 9)
            throw new ArgumentOutOfRangeException(nameof(precision), "Precision must be between 0 and 9.");

        _precision = precision;
        var unitsPerSecond = (long)Math.Pow(10, precision);
        _unitsPerDay = SecondsPerDay * unitsPerSecond;
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
    public TimeOnly ReadValue(ref ProtocolReader reader)
    {
        var value = reader.ReadInt64();
        if ((ulong)value >= (ulong)_unitsPerDay)
            throw new OverflowException(
                $"Time64({_precision}) value {value} is outside the representable range [0, {_unitsPerDay}); use the raw Int64 reader for negative or wrap-around times.");

        long ticks;
        if (_precision > 7)
        {
            var divisor = (long)Math.Pow(10, _precision - 7);
            ticks = value / divisor;
        }
        else
        {
            ticks = value * _ticksPerUnit;
        }

        return new TimeOnly(ticks);
    }

    /// <inheritdoc />
    public TypedColumn<TimeOnly> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<TimeOnly>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = ReadValue(ref reader);
        }
        return new TypedColumn<TimeOnly>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
