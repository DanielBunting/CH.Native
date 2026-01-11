using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Decimal64 values.
/// Decimal64 is stored as Int64 with a scale factor.
/// </summary>
public sealed class Decimal64ColumnWriter : IColumnWriter<decimal>
{
    private readonly int _scale;
    private readonly decimal _multiplier;

    /// <summary>
    /// Creates a Decimal64 writer with the specified scale.
    /// </summary>
    /// <param name="scale">The number of decimal places (0-18).</param>
    public Decimal64ColumnWriter(int scale)
    {
        if (scale < 0 || scale > 18)
            throw new ArgumentOutOfRangeException(nameof(scale), "Scale must be between 0 and 18 for Decimal64.");

        _scale = scale;
        _multiplier = (decimal)Math.Pow(10, scale);
    }

    /// <inheritdoc />
    public string TypeName => $"Decimal64({_scale})";

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
        var scaled = value * _multiplier;
        var longValue = (long)Math.Round(scaled);
        writer.WriteInt64(longValue);
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
