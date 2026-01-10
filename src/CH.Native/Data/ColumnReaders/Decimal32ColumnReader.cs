using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Decimal32 values.
/// Decimal32 is stored as Int32 with a scale factor.
/// </summary>
public sealed class Decimal32ColumnReader : IColumnReader<decimal>
{
    private readonly int _scale;
    private readonly decimal _divisor;

    /// <summary>
    /// Creates a Decimal32 reader with the specified scale.
    /// </summary>
    /// <param name="scale">The number of decimal places (0-9).</param>
    public Decimal32ColumnReader(int scale)
    {
        if (scale < 0 || scale > 9)
            throw new ArgumentOutOfRangeException(nameof(scale), "Scale must be between 0 and 9 for Decimal32.");

        _scale = scale;
        _divisor = (decimal)Math.Pow(10, scale);
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
    public decimal ReadValue(ref ProtocolReader reader)
    {
        var raw = reader.ReadInt32();
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
