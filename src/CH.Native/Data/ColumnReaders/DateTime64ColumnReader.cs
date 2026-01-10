using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for DateTime64 values.
/// DateTime64 stores high-precision timestamps as Int64 with configurable precision (0-9).
/// </summary>
public sealed class DateTime64ColumnReader : IColumnReader<DateTime>
{
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private readonly int _precision;
    private readonly string? _timezone;
    private readonly long _ticksPerUnit;

    /// <summary>
    /// Creates a DateTime64 reader with the specified precision and optional timezone.
    /// </summary>
    /// <param name="precision">The precision (0-9). 0=seconds, 3=milliseconds, 6=microseconds, 9=nanoseconds.</param>
    /// <param name="timezone">Optional timezone name (stored but not applied to returned DateTime).</param>
    public DateTime64ColumnReader(int precision, string? timezone = null)
    {
        if (precision < 0 || precision > 9)
            throw new ArgumentOutOfRangeException(nameof(precision), "Precision must be between 0 and 9.");

        _precision = precision;
        _timezone = timezone;

        // Calculate ticks per unit based on precision
        // TimeSpan.TicksPerSecond = 10,000,000 (100 nanosecond intervals)
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
    public DateTime ReadValue(ref ProtocolReader reader)
    {
        var value = reader.ReadInt64();

        // Handle high-precision values where ticks per unit would be < 1
        if (_precision > 7)
        {
            var divisor = (long)Math.Pow(10, _precision - 7);
            value /= divisor;
            return UnixEpoch.AddTicks(value);
        }

        return UnixEpoch.AddTicks(value * _ticksPerUnit);
    }

    /// <inheritdoc />
    public TypedColumn<DateTime> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<DateTime>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = ReadValue(ref reader);
        }
        return new TypedColumn<DateTime>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
