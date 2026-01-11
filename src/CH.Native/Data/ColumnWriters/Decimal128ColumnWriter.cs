using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Decimal128 values.
/// Decimal128 is stored as Int128 with a scale factor.
/// </summary>
public sealed class Decimal128ColumnWriter : IColumnWriter<decimal>
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
        var int128Value = DecimalToInt128(value, _scale);
        writer.WriteInt128(int128Value);
    }

    private static Int128 DecimalToInt128(decimal value, int scale)
    {
        // Apply scale by multiplying
        var multiplier = (decimal)Math.Pow(10, scale);
        var scaled = value * multiplier;

        // Handle negative values
        bool isNegative = scaled < 0;
        if (isNegative)
            scaled = -scaled;

        // Round to nearest integer
        scaled = Math.Round(scaled);

        // Convert to Int128
        // Decimal max is ~79,228,162,514,264,337,593,543,950,335
        // which fits in Int128
        // 2^64 = 18446744073709551616
        const decimal twoTo64 = 18446744073709551616m;
        var low = (ulong)(scaled % twoTo64);
        var high = (ulong)(scaled / twoTo64);

        var result = new Int128((ulong)high, low);
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
