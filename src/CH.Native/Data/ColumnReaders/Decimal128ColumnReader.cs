using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Decimal128 values.
/// Decimal128 is stored as Int128 with a scale factor.
/// </summary>
/// <remarks>
/// .NET decimal has a maximum precision of 28-29 significant digits.
/// Large Decimal128 values may lose precision when converted to decimal.
/// </remarks>
public sealed class Decimal128ColumnReader : IColumnReader<decimal>
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
    public Type ClrType => typeof(decimal);

    /// <summary>
    /// Gets the scale (number of decimal places).
    /// </summary>
    public int Scale => _scale;

    /// <inheritdoc />
    public decimal ReadValue(ref ProtocolReader reader)
    {
        var raw = reader.ReadInt128();
        return Int128ToDecimal(raw, _scale);
    }

    private static decimal Int128ToDecimal(Int128 value, int scale)
    {
        // Handle negative values
        bool isNegative = value < 0;
        if (isNegative)
            value = -value;

        // Convert Int128 to decimal
        // This may lose precision for very large values
        var high = (ulong)(value >> 64);
        var low = (ulong)value;

        decimal result;
        if (high == 0)
        {
            // Fits in ulong
            result = low;
        }
        else
        {
            // Need to handle the high part
            // 2^64 as decimal
            const decimal twoTo64 = 18446744073709551616m;
            result = high * twoTo64 + low;
        }

        // Apply scale
        if (scale > 0)
        {
            result /= (decimal)Math.Pow(10, scale);
        }

        return isNegative ? -result : result;
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
