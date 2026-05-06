using CH.Native.Exceptions;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Decimal32 values.
/// Decimal32 is stored as Int32 with a scale factor.
/// </summary>
internal sealed class Decimal32ColumnWriter : IColumnWriter<decimal>
{
    private readonly int _scale;
    private readonly decimal _multiplier;

    /// <summary>
    /// Creates a Decimal32 writer with the specified scale.
    /// </summary>
    /// <param name="scale">The number of decimal places (0-9).</param>
    public Decimal32ColumnWriter(int scale)
    {
        if (scale < 0 || scale > 9)
            throw new ArgumentOutOfRangeException(nameof(scale), "Scale must be between 0 and 9 for Decimal32.");

        _scale = scale;
        _multiplier = (decimal)Math.Pow(10, scale);
    }

    /// <inheritdoc />
    public string TypeName => $"Decimal32({_scale})";

    /// <inheritdoc />
    public Type ClrType => typeof(decimal);

    /// <summary>
    /// Gets the scale (number of decimal places).
    /// </summary>
    public int Scale => _scale;

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, decimal[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, decimal value)
    {
        // .NET's decimal-to-int cast is already checked, so an out-of-range
        // value raises OverflowException at runtime. Surface that as the
        // typed protocol exception so the resilience layer doesn't classify
        // it as a transient retry candidate (it isn't — same input would
        // overflow on every retry).
        var scaled = value * _multiplier;
        int intValue;
        try
        {
            intValue = (int)Math.Round(scaled);
        }
        catch (OverflowException ex)
        {
            throw new ClickHouseProtocolException(
                $"Decimal32({_scale}) cannot represent value {value} after scaling — " +
                $"scaled magnitude exceeds Int32 range.", ex);
        }
        writer.WriteInt32(intValue);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, (decimal)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        WriteValue(ref writer, (decimal)value!);
    }
}
