using System.Buffers;
using System.Numerics;
using CH.Native.Numerics;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Decimal128 values.
/// Decimal128 is stored as Int128 with a scale factor.
/// Returns <see cref="ClickHouseDecimal"/> to preserve full 38-digit precision.
/// </summary>
public sealed class Decimal128ColumnReader : IColumnReader<ClickHouseDecimal>
{
    private readonly int _scale;

    /// <summary>
    /// Creates a Decimal128 reader with the specified scale.
    /// </summary>
    /// <param name="scale">The number of decimal places (0-38).</param>
    public Decimal128ColumnReader(int scale)
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
    public ClickHouseDecimal ReadValue(ref ProtocolReader reader)
    {
        var raw = reader.ReadInt128();
        var bigValue = (BigInteger)raw;
        return new ClickHouseDecimal(bigValue, _scale);
    }

    /// <inheritdoc />
    public TypedColumn<ClickHouseDecimal> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<ClickHouseDecimal>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = ReadValue(ref reader);
        }
        return new TypedColumn<ClickHouseDecimal>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
