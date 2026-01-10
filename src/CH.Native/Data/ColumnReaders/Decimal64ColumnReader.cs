using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Decimal64 values.
/// Decimal64 is stored as Int64 with a scale factor.
/// </summary>
public sealed class Decimal64ColumnReader : IColumnReader<decimal>
{
    private readonly int _scale;
    private readonly decimal _divisor;

    /// <summary>
    /// Creates a Decimal64 reader with the specified scale.
    /// </summary>
    /// <param name="scale">The number of decimal places (0-18).</param>
    public Decimal64ColumnReader(int scale)
    {
        if (scale < 0 || scale > 18)
            throw new ArgumentOutOfRangeException(nameof(scale), "Scale must be between 0 and 18 for Decimal64.");

        _scale = scale;
        _divisor = (decimal)Math.Pow(10, scale);
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
    public decimal ReadValue(ref ProtocolReader reader)
    {
        var raw = reader.ReadInt64();
        return raw / _divisor;
    }

    /// <inheritdoc />
    public TypedColumn<decimal> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<decimal>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = ReadValue(ref reader);
        }
        return new TypedColumn<decimal>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
