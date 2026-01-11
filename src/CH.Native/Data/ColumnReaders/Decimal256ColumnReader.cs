using System.Buffers;
using System.Numerics;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Decimal256 values.
/// Decimal256 is stored as a 256-bit integer with a scale factor.
/// </summary>
/// <remarks>
/// .NET decimal has a maximum precision of 28-29 significant digits.
/// Decimal256 values will almost certainly lose precision when converted to decimal.
/// For full precision, consider using the raw byte reader and BigInteger.
/// </remarks>
public sealed class Decimal256ColumnReader : IColumnReader<decimal>
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
    public Type ClrType => typeof(decimal);

    /// <summary>
    /// Gets the scale (number of decimal places).
    /// </summary>
    public int Scale => _scale;

    /// <inheritdoc />
    public decimal ReadValue(ref ProtocolReader reader)
    {
        // Read 256 bits (32 bytes) as four 64-bit little-endian values
        var bytes = reader.ReadBytes(32);

        // Convert to BigInteger for arbitrary precision arithmetic
        // The bytes are in little-endian order, signed
        var bigInt = new BigInteger(bytes.Span, isUnsigned: false, isBigEndian: false);

        return BigIntegerToDecimal(bigInt, _scale);
    }

    private static decimal BigIntegerToDecimal(BigInteger value, int scale)
    {
        // Handle sign
        bool isNegative = value.Sign < 0;
        if (isNegative)
            value = -value;

        // Apply scale by dividing
        if (scale > 0)
        {
            var divisor = BigInteger.Pow(10, scale);
            var (quotient, remainder) = BigInteger.DivRem(value, divisor);

            // Convert quotient to decimal
            decimal result = BigIntegerToDecimalUnchecked(quotient);

            // Add fractional part if there's a remainder
            if (remainder != 0)
            {
                decimal fraction = BigIntegerToDecimalUnchecked(remainder) / (decimal)Math.Pow(10, scale);
                result += fraction;
            }

            return isNegative ? -result : result;
        }
        else
        {
            decimal result = BigIntegerToDecimalUnchecked(value);
            return isNegative ? -result : result;
        }
    }

    private static decimal BigIntegerToDecimalUnchecked(BigInteger value)
    {
        // Convert BigInteger to decimal, clamping to decimal.MaxValue if necessary
        if (value > new BigInteger(decimal.MaxValue))
            return decimal.MaxValue;
        if (value < new BigInteger(decimal.MinValue))
            return decimal.MinValue;

        // For values that fit in decimal's range, we can safely convert
        // by extracting 64-bit chunks
        const decimal twoTo64 = 18446744073709551616m;

        decimal result = 0;
        var remaining = value;

        // Process 64 bits at a time
        while (remaining > ulong.MaxValue)
        {
            var low = (ulong)(remaining & ulong.MaxValue);
            remaining >>= 64;
            result = result * twoTo64 + low;
        }

        // Handle the remaining bits
        if (remaining > 0)
        {
            result = result * twoTo64 + (ulong)remaining;
        }
        else if (result == 0)
        {
            // Value was small enough to fit in ulong from the start
            result = (ulong)value;
        }

        return result;
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
