using System.Globalization;
using System.Numerics;

namespace CH.Native.Numerics;

/// <summary>
/// Represents a decimal value with arbitrary precision, backed by a <see cref="BigInteger"/> mantissa and an integer scale.
/// This type preserves the full precision of ClickHouse Decimal128 (38 digits) and Decimal256 (76 digits),
/// which exceed the 28-29 significant digit limit of .NET <see cref="decimal"/>.
/// </summary>
public readonly struct ClickHouseDecimal
    : IComparable, IComparable<ClickHouseDecimal>, IComparable<decimal>,
      IFormattable, IConvertible, IEquatable<ClickHouseDecimal>
{
    /// <summary>
    /// The unscaled integer value. The actual value is <c>Mantissa * 10^(-Scale)</c>.
    /// </summary>
    public BigInteger Mantissa { get; }

    /// <summary>
    /// The number of decimal places (non-negative).
    /// </summary>
    public int Scale { get; }

    /// <summary>
    /// A <see cref="ClickHouseDecimal"/> representing zero.
    /// </summary>
    public static readonly ClickHouseDecimal Zero = new(BigInteger.Zero, 0);

    /// <summary>
    /// A <see cref="ClickHouseDecimal"/> representing one.
    /// </summary>
    public static readonly ClickHouseDecimal One = new(BigInteger.One, 0);

    /// <summary>
    /// Initializes a new instance with the specified mantissa and scale.
    /// </summary>
    /// <param name="mantissa">The unscaled integer value.</param>
    /// <param name="scale">The number of decimal places (must be non-negative).</param>
    public ClickHouseDecimal(BigInteger mantissa, int scale)
    {
        if (scale < 0)
            throw new ArgumentOutOfRangeException(nameof(scale), "Scale must be non-negative.");
        Mantissa = mantissa;
        Scale = scale;
    }

    // -------------------------------------------------------------------
    //  Helpers
    // -------------------------------------------------------------------

    /// <summary>
    /// Returns the number of decimal digits in the absolute value of <paramref name="value"/>.
    /// </summary>
    public static int NumberOfDigits(BigInteger value)
    {
        if (value.IsZero) return 1;
        if (value.Sign < 0) value = -value;

        // Fast path for small values
        if (value < 10) return 1;

        // BigInteger.ToString is allocation-heavy for huge values; use log-based estimate
        // then verify with a power-of-ten comparison.
        var digits = (int)Math.Ceiling(BigInteger.Log10(value + 1));
        if (BigInteger.Pow(10, digits) <= value) digits++;
        return digits;
    }

    /// <summary>
    /// Strips trailing fractional zeros, reducing scale accordingly.
    /// </summary>
    public ClickHouseDecimal Normalize()
    {
        if (Mantissa.IsZero) return new ClickHouseDecimal(BigInteger.Zero, 0);

        var mantissa = Mantissa;
        var scale = Scale;
        while (scale > 0)
        {
            var (q, r) = BigInteger.DivRem(mantissa, 10);
            if (!r.IsZero) break;
            mantissa = q;
            scale--;
        }
        return new ClickHouseDecimal(mantissa, scale);
    }

    /// <summary>
    /// Truncates the value to an integer (removes all fractional digits).
    /// </summary>
    public ClickHouseDecimal Truncate()
    {
        if (Scale == 0) return this;
        var divisor = BigInteger.Pow(10, Scale);
        var truncated = BigInteger.Divide(Mantissa, divisor);
        return new ClickHouseDecimal(truncated, 0);
    }

    private static (BigInteger left, BigInteger right, int scale) AlignScales(ClickHouseDecimal a, ClickHouseDecimal b)
    {
        if (a.Scale == b.Scale)
            return (a.Mantissa, b.Mantissa, a.Scale);

        if (a.Scale > b.Scale)
        {
            var factor = BigInteger.Pow(10, a.Scale - b.Scale);
            return (a.Mantissa, b.Mantissa * factor, a.Scale);
        }
        else
        {
            var factor = BigInteger.Pow(10, b.Scale - a.Scale);
            return (a.Mantissa * factor, b.Mantissa, b.Scale);
        }
    }

    // -------------------------------------------------------------------
    //  Implicit conversions FROM primitive types
    // -------------------------------------------------------------------

    public static implicit operator ClickHouseDecimal(int value) =>
        new(new BigInteger(value), 0);

    public static implicit operator ClickHouseDecimal(long value) =>
        new(new BigInteger(value), 0);

    public static implicit operator ClickHouseDecimal(decimal value)
    {
        var bits = decimal.GetBits(value);
        var lo = (uint)bits[0];
        var mid = (uint)bits[1];
        var hi = (uint)bits[2];
        var flags = bits[3];
        var isNegative = (flags & unchecked((int)0x80000000)) != 0;
        var scale = (flags >> 16) & 0xFF;

        var magnitude = new BigInteger(lo)
                      + new BigInteger(mid) * ((BigInteger)1 << 32)
                      + new BigInteger(hi) * ((BigInteger)1 << 64);

        return new ClickHouseDecimal(isNegative ? -magnitude : magnitude, scale);
    }

    public static implicit operator ClickHouseDecimal(double value)
    {
        // Use decimal as intermediate when within range
        if (value is >= (double)decimal.MinValue and <= (double)decimal.MaxValue and not (double.NaN or double.PositiveInfinity or double.NegativeInfinity))
        {
            try
            {
                return (decimal)value;
            }
            catch (OverflowException)
            {
                // Fall through to string-based parsing
            }
        }

        // For values outside decimal range, use string round-trip
        var s = value.ToString("R", CultureInfo.InvariantCulture);
        return Parse(s, CultureInfo.InvariantCulture);
    }

    // -------------------------------------------------------------------
    //  Explicit conversions TO primitive types
    // -------------------------------------------------------------------

    /// <summary>
    /// Converts to .NET <see cref="decimal"/>, truncating to fit .NET decimal precision.
    /// Unlike the ClickHouse.Driver version, this does NOT throw on overflow —
    /// it clamps to <see cref="decimal.MaxValue"/> / <see cref="decimal.MinValue"/>.
    /// </summary>
    public static explicit operator decimal(ClickHouseDecimal value)
    {
        var mantissa = value.Mantissa;
        var scale = value.Scale;
        var isNegative = mantissa.Sign < 0;
        if (isNegative) mantissa = -mantissa;

        if (scale > 0)
        {
            var divisor = BigInteger.Pow(10, scale);
            var (quotient, remainder) = BigInteger.DivRem(mantissa, divisor);

            if (!TryBigIntegerToDecimal(quotient, out var intPart))
                return isNegative ? decimal.MinValue : decimal.MaxValue;

            // Build fractional part — truncate scale to max 28 (decimal limit)
            var effectiveScale = Math.Min(scale, 28);
            if (scale > 28)
            {
                var reduce = BigInteger.Pow(10, scale - 28);
                remainder = BigInteger.Divide(remainder, reduce);
            }

            if (!remainder.IsZero && TryBigIntegerToUnsignedDecimal(remainder, (byte)effectiveScale, out var fracPart))
                intPart += fracPart;

            return isNegative ? -intPart : intPart;
        }

        if (!TryBigIntegerToDecimal(mantissa, out var result))
            return isNegative ? decimal.MinValue : decimal.MaxValue;

        return isNegative ? -result : result;
    }

    public static explicit operator double(ClickHouseDecimal value)
    {
        // For small values, go through decimal for precision
        if (value.Scale <= 28 && NumberOfDigits(BigInteger.Abs(value.Mantissa)) <= 28)
        {
            try { return (double)(decimal)value; }
            catch { /* fall through */ }
        }
        return double.Parse(value.ToString(null, CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
    }

    public static explicit operator float(ClickHouseDecimal value) =>
        (float)(double)value;

    public static explicit operator int(ClickHouseDecimal value) =>
        (int)value.Truncate().Mantissa;

    public static explicit operator long(ClickHouseDecimal value) =>
        (long)value.Truncate().Mantissa;

    public static explicit operator BigInteger(ClickHouseDecimal value) =>
        value.Truncate().Mantissa;

    // -------------------------------------------------------------------
    //  Arithmetic operators
    // -------------------------------------------------------------------

    public static ClickHouseDecimal operator +(ClickHouseDecimal a, ClickHouseDecimal b)
    {
        var (left, right, scale) = AlignScales(a, b);
        return new ClickHouseDecimal(left + right, scale);
    }

    public static ClickHouseDecimal operator -(ClickHouseDecimal a, ClickHouseDecimal b)
    {
        var (left, right, scale) = AlignScales(a, b);
        return new ClickHouseDecimal(left - right, scale);
    }

    public static ClickHouseDecimal operator -(ClickHouseDecimal value) =>
        new(-value.Mantissa, value.Scale);

    public static ClickHouseDecimal operator *(ClickHouseDecimal a, ClickHouseDecimal b) =>
        new(a.Mantissa * b.Mantissa, a.Scale + b.Scale);

    public static ClickHouseDecimal operator /(ClickHouseDecimal a, ClickHouseDecimal b)
    {
        if (b.Mantissa.IsZero)
            throw new DivideByZeroException();

        // Increase precision for the result
        const int extraPrecision = 38;
        var scaledMantissa = a.Mantissa * BigInteger.Pow(10, extraPrecision);
        var resultMantissa = BigInteger.Divide(scaledMantissa, b.Mantissa);
        return new ClickHouseDecimal(resultMantissa, a.Scale - b.Scale + extraPrecision);
    }

    public static ClickHouseDecimal operator %(ClickHouseDecimal a, ClickHouseDecimal b)
    {
        var (left, right, scale) = AlignScales(a, b);
        return new ClickHouseDecimal(left % right, scale);
    }

    // -------------------------------------------------------------------
    //  Comparison operators
    // -------------------------------------------------------------------

    public static bool operator ==(ClickHouseDecimal a, ClickHouseDecimal b) => a.CompareTo(b) == 0;
    public static bool operator !=(ClickHouseDecimal a, ClickHouseDecimal b) => a.CompareTo(b) != 0;
    public static bool operator <(ClickHouseDecimal a, ClickHouseDecimal b) => a.CompareTo(b) < 0;
    public static bool operator >(ClickHouseDecimal a, ClickHouseDecimal b) => a.CompareTo(b) > 0;
    public static bool operator <=(ClickHouseDecimal a, ClickHouseDecimal b) => a.CompareTo(b) <= 0;
    public static bool operator >=(ClickHouseDecimal a, ClickHouseDecimal b) => a.CompareTo(b) >= 0;

    // -------------------------------------------------------------------
    //  IComparable / IEquatable
    // -------------------------------------------------------------------

    public int CompareTo(ClickHouseDecimal other)
    {
        var (left, right, _) = AlignScales(this, other);
        return left.CompareTo(right);
    }

    public int CompareTo(decimal other) => CompareTo((ClickHouseDecimal)other);

    public int CompareTo(object? obj) => obj switch
    {
        null => 1,
        ClickHouseDecimal chd => CompareTo(chd),
        decimal d => CompareTo(d),
        _ => throw new ArgumentException($"Object must be of type {nameof(ClickHouseDecimal)}.", nameof(obj)),
    };

    public bool Equals(ClickHouseDecimal other) => CompareTo(other) == 0;

    public override bool Equals(object? obj) => obj is ClickHouseDecimal other && Equals(other);

    public override int GetHashCode()
    {
        var normalized = Normalize();
        return HashCode.Combine(normalized.Mantissa, normalized.Scale);
    }

    // -------------------------------------------------------------------
    //  ToString / Parse
    // -------------------------------------------------------------------

    public override string ToString() => ToString(null, CultureInfo.InvariantCulture);

    public string ToString(string? format, IFormatProvider? formatProvider)
    {
        if (Scale == 0) return Mantissa.ToString(format, formatProvider);

        var isNegative = Mantissa.Sign < 0;
        var absMantissa = isNegative ? -Mantissa : Mantissa;
        var divisor = BigInteger.Pow(10, Scale);
        var (quotient, remainder) = BigInteger.DivRem(absMantissa, divisor);

        var fracStr = remainder.ToString().PadLeft(Scale, '0');

        var prefix = isNegative ? "-" : "";
        return $"{prefix}{quotient}.{fracStr}";
    }

    /// <summary>
    /// Parses a decimal string into a <see cref="ClickHouseDecimal"/>.
    /// </summary>
    public static ClickHouseDecimal Parse(string s) => Parse(s, CultureInfo.InvariantCulture);

    /// <summary>
    /// Parses a decimal string into a <see cref="ClickHouseDecimal"/>.
    /// </summary>
    public static ClickHouseDecimal Parse(string s, IFormatProvider? provider)
    {
        if (string.IsNullOrWhiteSpace(s))
            throw new FormatException("Input string was not in a correct format.");

        s = s.Trim();

        var isNegative = false;
        if (s.Length > 0 && s[0] == '-')
        {
            isNegative = true;
            s = s[1..];
        }
        else if (s.Length > 0 && s[0] == '+')
        {
            s = s[1..];
        }

        var dotIndex = s.IndexOf('.');
        int scale;
        BigInteger mantissa;
        if (dotIndex >= 0)
        {
            var intPart = s[..dotIndex];
            var fracPart = s[(dotIndex + 1)..];
            scale = fracPart.Length;
            var combined = intPart + fracPart;
            mantissa = BigInteger.Parse(combined, NumberStyles.None, provider);
        }
        else
        {
            mantissa = BigInteger.Parse(s, NumberStyles.None, provider);
            scale = 0;
        }

        if (isNegative)
            mantissa = -mantissa;

        return new ClickHouseDecimal(mantissa, scale);
    }

    // -------------------------------------------------------------------
    //  IConvertible
    // -------------------------------------------------------------------

    TypeCode IConvertible.GetTypeCode() => TypeCode.Object;

    bool IConvertible.ToBoolean(IFormatProvider? provider) => !Mantissa.IsZero;
    byte IConvertible.ToByte(IFormatProvider? provider) => (byte)(int)this;
    char IConvertible.ToChar(IFormatProvider? provider) => throw new InvalidCastException();
    DateTime IConvertible.ToDateTime(IFormatProvider? provider) => throw new InvalidCastException();
    decimal IConvertible.ToDecimal(IFormatProvider? provider) => (decimal)this;
    double IConvertible.ToDouble(IFormatProvider? provider) => (double)this;
    short IConvertible.ToInt16(IFormatProvider? provider) => (short)(int)this;
    int IConvertible.ToInt32(IFormatProvider? provider) => (int)this;
    long IConvertible.ToInt64(IFormatProvider? provider) => (long)this;
    sbyte IConvertible.ToSByte(IFormatProvider? provider) => (sbyte)(int)this;
    float IConvertible.ToSingle(IFormatProvider? provider) => (float)this;
    string IConvertible.ToString(IFormatProvider? provider) => ToString(null, provider);
    ushort IConvertible.ToUInt16(IFormatProvider? provider) => (ushort)(int)this;
    uint IConvertible.ToUInt32(IFormatProvider? provider) => (uint)(long)this;
    ulong IConvertible.ToUInt64(IFormatProvider? provider) => (ulong)(long)this;

    object IConvertible.ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(ClickHouseDecimal)) return this;
        if (conversionType == typeof(decimal)) return (decimal)this;
        if (conversionType == typeof(double)) return (double)this;
        if (conversionType == typeof(float)) return (float)this;
        if (conversionType == typeof(int)) return (int)this;
        if (conversionType == typeof(long)) return (long)this;
        if (conversionType == typeof(string)) return ToString();
        if (conversionType == typeof(BigInteger)) return (BigInteger)this;
        if (conversionType == typeof(bool)) return !Mantissa.IsZero;

        throw new InvalidCastException($"Cannot convert ClickHouseDecimal to {conversionType.Name}.");
    }

    // -------------------------------------------------------------------
    //  IFormattable
    // -------------------------------------------------------------------

    // Already implemented via ToString(string?, IFormatProvider?)

    // -------------------------------------------------------------------
    //  Private helpers
    // -------------------------------------------------------------------

    private static bool TryBigIntegerToDecimal(BigInteger value, out decimal result)
    {
        result = 0m;
        if (value.IsZero) return true;
        if (value > new BigInteger(decimal.MaxValue))
            return false;

        var bytes = value.ToByteArray(isUnsigned: true, isBigEndian: false);
        if (bytes.Length > 12) return false;

        int lo = 0, mid = 0, hi = 0;
        if (bytes.Length > 0) lo = ReadInt32LE(bytes, 0);
        if (bytes.Length > 4) mid = ReadInt32LE(bytes, 4);
        if (bytes.Length > 8) hi = ReadInt32LE(bytes, 8);

        result = new decimal(lo, mid, hi, false, 0);
        return true;
    }

    private static bool TryBigIntegerToUnsignedDecimal(BigInteger value, byte scale, out decimal result)
    {
        result = 0m;
        if (value.IsZero) return true;
        if (value > new BigInteger(decimal.MaxValue))
            return false;

        var bytes = value.ToByteArray(isUnsigned: true, isBigEndian: false);
        if (bytes.Length > 12) return false;

        int lo = 0, mid = 0, hi = 0;
        if (bytes.Length > 0) lo = ReadInt32LE(bytes, 0);
        if (bytes.Length > 4) mid = ReadInt32LE(bytes, 4);
        if (bytes.Length > 8) hi = ReadInt32LE(bytes, 8);

        result = new decimal(lo, mid, hi, false, scale);
        return true;
    }

    private static int ReadInt32LE(byte[] source, int offset)
    {
        var length = Math.Min(4, source.Length - offset);
        if (length <= 0) return 0;
        Span<byte> buf = stackalloc byte[4];
        source.AsSpan(offset, length).CopyTo(buf);
        return BitConverter.ToInt32(buf);
    }
}
