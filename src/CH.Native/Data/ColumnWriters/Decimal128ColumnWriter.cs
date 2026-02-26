using System.Numerics;
using CH.Native.Numerics;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Decimal128 values.
/// Decimal128 is stored as Int128 with a scale factor.
/// </summary>
public sealed class Decimal128ColumnWriter : IColumnWriter<ClickHouseDecimal>
{
    private readonly int _scale;

    /// <summary>
    /// Creates a Decimal128 writer with the specified scale.
    /// </summary>
    /// <param name="scale">The number of decimal places (0-38).</param>
    public Decimal128ColumnWriter(int scale)
    {
        if (scale < 0 || scale > 38)
            throw new ArgumentOutOfRangeException(nameof(scale), "Scale must be between 0 and 38 for Decimal128.");

        _scale = scale;
    }

    /// <inheritdoc />
    public string TypeName => $"Decimal128({_scale})";

    /// <inheritdoc />
    public Type ClrType => typeof(ClickHouseDecimal);

    /// <summary>
    /// Gets the scale (number of decimal places).
    /// </summary>
    public int Scale => _scale;

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, ClickHouseDecimal[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, ClickHouseDecimal value)
    {
        var scaled = RescaleToTargetScale(value, _scale);
        var int128Value = (Int128)scaled;
        writer.WriteInt128(int128Value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            ((IColumnWriter)this).WriteValue(ref writer, values[i]);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        var chd = value switch
        {
            ClickHouseDecimal d => d,
            decimal d => (ClickHouseDecimal)d,
            _ => throw new InvalidCastException($"Cannot convert {value?.GetType().Name ?? "null"} to ClickHouseDecimal.")
        };
        WriteValue(ref writer, chd);
    }

    private static BigInteger RescaleToTargetScale(ClickHouseDecimal value, int targetScale)
    {
        var mantissa = value.Mantissa;
        var currentScale = value.Scale;

        if (currentScale == targetScale)
            return mantissa;

        if (currentScale < targetScale)
        {
            // Need more decimal places — multiply
            return mantissa * BigInteger.Pow(10, targetScale - currentScale);
        }

        // Need fewer decimal places — divide (truncate)
        return BigInteger.Divide(mantissa, BigInteger.Pow(10, currentScale - targetScale));
    }
}
