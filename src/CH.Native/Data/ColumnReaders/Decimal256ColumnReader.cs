using System.Buffers;
using System.Numerics;
using CH.Native.Numerics;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Decimal256 values.
/// Decimal256 is stored as a 256-bit integer with a scale factor.
/// Returns <see cref="ClickHouseDecimal"/> to preserve full 76-digit precision.
/// </summary>
public sealed class Decimal256ColumnReader : IColumnReader<ClickHouseDecimal>
{
    private readonly int _scale;

    /// <summary>
    /// Creates a Decimal256 reader with the specified scale.
    /// </summary>
    /// <param name="scale">The number of decimal places (0-76).</param>
    public Decimal256ColumnReader(int scale)
    {
        if (scale < 0 || scale > 76)
            throw new ArgumentOutOfRangeException(nameof(scale), "Scale must be between 0 and 76 for Decimal256.");

        _scale = scale;
    }

    /// <inheritdoc />
    public string TypeName => $"Decimal256({_scale})";

    /// <inheritdoc />
    public Type ClrType => typeof(ClickHouseDecimal);

    /// <summary>
    /// Gets the scale (number of decimal places).
    /// </summary>
    public int Scale => _scale;

    /// <inheritdoc />
    public ClickHouseDecimal ReadValue(ref ProtocolReader reader)
    {
        // Read 256 bits (32 bytes) as signed little-endian BigInteger
        var bytes = reader.ReadBytes(32);
        var bigInt = new BigInteger(bytes.Span, isUnsigned: false, isBigEndian: false);
        return new ClickHouseDecimal(bigInt, _scale);
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
