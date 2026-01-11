using System.Numerics;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Decimal256 values.
/// Decimal256 is stored as 256-bit integer with a scale factor.
/// </summary>
public sealed class Decimal256ColumnWriter : IColumnWriter<decimal>
{
    private readonly int _scale;

    /// <summary>
    /// Creates a Decimal256 writer with the specified scale.
    /// </summary>
    /// <param name="scale">The number of decimal places (0-76).</param>
    public Decimal256ColumnWriter(int scale)
    {
        if (scale < 0 || scale > 76)
            throw new ArgumentOutOfRangeException(nameof(scale), "Scale must be between 0 and 76 for Decimal256.");

        _scale = scale;
    }

    /// <inheritdoc />
    public string TypeName => $"Decimal256({_scale})";

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
        var bigIntValue = DecimalToBigInteger(value, _scale);
        writer.WriteInt256(bigIntValue);
    }

    private static BigInteger DecimalToBigInteger(decimal value, int scale)
    {
        // Apply scale by multiplying
        // We need to work with BigInteger for precision
        var multiplier = BigInteger.Pow(10, scale);

        // Convert decimal to BigInteger
        // Decimal is stored as a 96-bit integer with a scale
        var bits = decimal.GetBits(value);
        var lo = (uint)bits[0];
        var mid = (uint)bits[1];
        var hi = (uint)bits[2];
        var flags = bits[3];

        var isNegative = (flags & 0x80000000) != 0;
        var decimalScale = (flags >> 16) & 0xFF;

        // Construct the magnitude
        var magnitude = new BigInteger(lo)
                      + new BigInteger(mid) * (BigInteger.One << 32)
                      + new BigInteger(hi) * (BigInteger.One << 64);

        // Apply the decimal's scale
        var divisor = BigInteger.Pow(10, decimalScale);

        // Calculate: (magnitude / divisor) * multiplier
        // To maintain precision: (magnitude * multiplier) / divisor
        var result = (magnitude * multiplier) / divisor;

        return isNegative ? -result : result;
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
