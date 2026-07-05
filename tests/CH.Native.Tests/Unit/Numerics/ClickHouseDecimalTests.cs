using System.Globalization;
using System.Numerics;
using CH.Native.Numerics;
using Xunit;

namespace CH.Native.Tests.Unit.Numerics;

/// <summary>
/// Unit coverage for <see cref="ClickHouseDecimal"/>, the arbitrary-precision decimal that backs
/// Decimal128/256 values beyond .NET <see cref="decimal"/> range. Ported and adapted from
/// ClickHouse.Driver's ClickHouseDecimalTests, pinning CH.Native's deliberate divergences
/// (clamp-not-throw to decimal, format ignored when a fraction is present, no auto-normalizing ctor).
/// </summary>
public class ClickHouseDecimalTests
{
    // ---- construction & statics ------------------------------------------------

    [Fact]
    public void Constructor_NegativeScale_Throws() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new ClickHouseDecimal(BigInteger.One, -1));

    [Fact]
    public void ZeroAndOne_HaveExpectedComponents()
    {
        Assert.Equal(BigInteger.Zero, ClickHouseDecimal.Zero.Mantissa);
        Assert.Equal(0, ClickHouseDecimal.Zero.Scale);
        Assert.Equal(BigInteger.One, ClickHouseDecimal.One.Mantissa);
        Assert.Equal(0, ClickHouseDecimal.One.Scale);
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(9, 1)]
    [InlineData(10, 2)]
    [InlineData(99, 2)]
    [InlineData(100, 3)]
    [InlineData(-12345, 5)]
    public void NumberOfDigits_CountsAbsoluteDigits(long value, int expected) =>
        Assert.Equal(expected, ClickHouseDecimal.NumberOfDigits(new BigInteger(value)));

    // ---- normalize / truncate --------------------------------------------------

    [Fact]
    public void Normalize_StripsTrailingFractionalZeros()
    {
        var normalized = new ClickHouseDecimal(new BigInteger(15000), 4).Normalize(); // 1.5000
        Assert.Equal(new BigInteger(15), normalized.Mantissa);
        Assert.Equal(1, normalized.Scale);
    }

    [Fact]
    public void Normalize_Zero_ReducesToScaleZero()
    {
        var normalized = new ClickHouseDecimal(BigInteger.Zero, 7).Normalize();
        Assert.Equal(BigInteger.Zero, normalized.Mantissa);
        Assert.Equal(0, normalized.Scale);
    }

    [Fact]
    public void Truncate_DropsFraction()
    {
        var truncated = new ClickHouseDecimal(new BigInteger(12399), 2).Truncate(); // 123.99 -> 123
        Assert.Equal(new BigInteger(123), truncated.Mantissa);
        Assert.Equal(0, truncated.Scale);
    }

    // ---- arithmetic ------------------------------------------------------------

    [Fact]
    public void Add_AlignsScales()
    {
        var result = new ClickHouseDecimal(new BigInteger(15), 1) + new ClickHouseDecimal(new BigInteger(2), 0); // 1.5 + 2
        Assert.Equal(new ClickHouseDecimal(new BigInteger(35), 1), result); // 3.5
    }

    [Fact]
    public void Subtract_AlignsScales()
    {
        var result = new ClickHouseDecimal(new BigInteger(5), 0) - new ClickHouseDecimal(new BigInteger(15), 1); // 5 - 1.5
        Assert.Equal(new ClickHouseDecimal(new BigInteger(35), 1), result); // 3.5
    }

    [Fact]
    public void Negate_FlipsSign()
    {
        var result = -new ClickHouseDecimal(new BigInteger(150), 2); // -(1.50)
        Assert.Equal(new BigInteger(-150), result.Mantissa);
        Assert.Equal(2, result.Scale);
    }

    [Fact]
    public void Multiply_SumsScales()
    {
        var result = new ClickHouseDecimal(new BigInteger(15), 1) * new ClickHouseDecimal(new BigInteger(2), 0); // 1.5 * 2
        Assert.Equal(new BigInteger(30), result.Mantissa);
        Assert.Equal(1, result.Scale); // 3.0
        Assert.Equal(ClickHouseDecimal.Parse("3"), result);
    }

    [Fact]
    public void Divide_ProducesHighPrecisionQuotient()
    {
        var oneThird = ClickHouseDecimal.One / new ClickHouseDecimal(new BigInteger(3), 0);
        // extraPrecision = 38 fractional digits, all threes.
        Assert.StartsWith("0.33333333333333333333", oneThird.ToString());
        // Round-trips back to ~1 when multiplied by 3.
        var almostOne = oneThird * new ClickHouseDecimal(new BigInteger(3), 0);
        Assert.True(ClickHouseDecimal.One - almostOne < ClickHouseDecimal.Parse("0.0000000001"));
    }

    [Fact]
    public void Divide_ByZero_Throws() =>
        Assert.Throws<DivideByZeroException>(() => ClickHouseDecimal.One / ClickHouseDecimal.Zero);

    [Fact]
    public void Modulo_AlignsScales()
    {
        var result = new ClickHouseDecimal(new BigInteger(55), 1) % new ClickHouseDecimal(new BigInteger(2), 0); // 5.5 % 2
        Assert.Equal(ClickHouseDecimal.Parse("1.5"), result);
    }

    // ---- comparison & equality -------------------------------------------------

    [Fact]
    public void Equality_IgnoresScale()
    {
        var a = new ClickHouseDecimal(new BigInteger(10), 1); // 1.0
        var b = new ClickHouseDecimal(new BigInteger(1), 0);  // 1
        Assert.True(a == b);
        Assert.True(a.Equals(b));
        Assert.False(a != b);
    }

    [Fact]
    public void GetHashCode_ConsistentAcrossScales()
    {
        var a = new ClickHouseDecimal(new BigInteger(10), 1); // 1.0
        var b = new ClickHouseDecimal(new BigInteger(1), 0);  // 1
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Theory]
    [InlineData("1", "2", -1)]
    [InlineData("2", "2", 0)]
    [InlineData("3.00", "3", 0)]
    [InlineData("5", "4.5", 1)]
    public void CompareTo_OrdersNumerically(string left, string right, int expectedSign) =>
        Assert.Equal(expectedSign, Math.Sign(ClickHouseDecimal.Parse(left).CompareTo(ClickHouseDecimal.Parse(right))));

    [Fact]
    public void CompareTo_Object_NullIsLess() =>
        Assert.Equal(1, ClickHouseDecimal.One.CompareTo(null));

    [Fact]
    public void CompareTo_Object_Decimal_Compares() =>
        Assert.Equal(0, ClickHouseDecimal.One.CompareTo((object)1m));

    [Fact]
    public void CompareTo_Object_WrongType_Throws() =>
        Assert.Throws<ArgumentException>(() => ClickHouseDecimal.One.CompareTo("not a number"));

    // ---- parse -----------------------------------------------------------------

    [Theory]
    [InlineData("0", 0, 0)]
    [InlineData("42", 42, 0)]
    [InlineData("-42", -42, 0)]
    [InlineData("+42", 42, 0)]
    [InlineData("1.50", 150, 2)]
    [InlineData("-0.001", -1, 3)]
    public void Parse_ValidStrings(string input, long expectedMantissa, int expectedScale)
    {
        var parsed = ClickHouseDecimal.Parse(input);
        Assert.Equal(new BigInteger(expectedMantissa), parsed.Mantissa);
        Assert.Equal(expectedScale, parsed.Scale);
    }

    [Fact]
    public void Parse_HugeValueBeyondDecimal_PreservesFullPrecision()
    {
        // 73 integer digits + 27 fractional digits — far beyond System.Decimal's 28-29 digits.
        var intPart = new string('9', 73);
        var fracPart = "123456789012345678901234567";
        var parsed = ClickHouseDecimal.Parse($"{intPart}.{fracPart}");
        Assert.Equal(27, parsed.Scale);
        Assert.Equal($"{intPart}.{fracPart}", parsed.ToString());
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Parse_EmptyOrWhitespace_Throws(string input) =>
        Assert.Throws<FormatException>(() => ClickHouseDecimal.Parse(input));

    [Theory]
    [InlineData("1,000")]   // NumberStyles.None rejects group separators
    [InlineData("1e5")]     // and exponents
    [InlineData("1 2")]
    [InlineData("abc")]
    public void Parse_RejectsSeparatorsAndExponents(string input) =>
        Assert.Throws<FormatException>(() => ClickHouseDecimal.Parse(input));

    // ---- ToString --------------------------------------------------------------

    [Fact]
    public void ToString_Default_UsesInvariantDotSeparator() =>
        Assert.Equal("1234.56", ClickHouseDecimal.Parse("1234.56").ToString());

    [Fact]
    public void ToString_Negative_RendersSign() =>
        Assert.Equal("-1.50", ClickHouseDecimal.Parse("-1.50").ToString());

    [Fact]
    public void ToString_FormatApplied_OnlyWhenNoFraction()
    {
        // Scale == 0: format/provider apply to the whole mantissa.
        Assert.Equal("1,234", new ClickHouseDecimal(new BigInteger(1234), 0).ToString("N0", CultureInfo.InvariantCulture));
        // Scale > 0: format/provider are IGNORED — plain digits with a '.' separator (CH.Native divergence).
        Assert.Equal("1234.56", ClickHouseDecimal.Parse("1234.56").ToString("N0", CultureInfo.InvariantCulture));
    }

    // ---- conversions FROM primitives -------------------------------------------

    [Fact]
    public void ImplicitFromInt_ScaleZero()
    {
        ClickHouseDecimal d = 42;
        Assert.Equal(new BigInteger(42), d.Mantissa);
        Assert.Equal(0, d.Scale);
    }

    [Fact]
    public void ImplicitFromLong_ScaleZero()
    {
        ClickHouseDecimal d = 9_000_000_000L;
        Assert.Equal(new BigInteger(9_000_000_000L), d.Mantissa);
        Assert.Equal(0, d.Scale);
    }

    [Fact]
    public void ImplicitFromDecimal_PreservesScale()
    {
        ClickHouseDecimal d = 1.50m; // decimal keeps the trailing zero -> scale 2
        Assert.Equal(new BigInteger(150), d.Mantissa);
        Assert.Equal(2, d.Scale);
    }

    [Fact]
    public void ImplicitFromDouble_RoundTrips()
    {
        ClickHouseDecimal d = 3.25;
        Assert.Equal(3.25, (double)d, 10);
    }

    // ---- conversions TO primitives ---------------------------------------------

    [Fact]
    public void ExplicitToDecimal_RoundTrips() =>
        Assert.Equal(123.456m, (decimal)ClickHouseDecimal.Parse("123.456"));

    [Fact]
    public void ExplicitToDecimal_Overflow_ClampsInsteadOfThrowing()
    {
        // 1e40 exceeds decimal.MaxValue; CH.Native clamps (driver would throw).
        var huge = ClickHouseDecimal.Parse("1" + new string('0', 40));
        Assert.Equal(decimal.MaxValue, (decimal)huge);
        var negHuge = ClickHouseDecimal.Parse("-1" + new string('0', 40));
        Assert.Equal(decimal.MinValue, (decimal)negHuge);
    }

    [Fact]
    public void ExplicitToDouble_And_Float()
    {
        var d = ClickHouseDecimal.Parse("2.5");
        Assert.Equal(2.5, (double)d);
        Assert.Equal(2.5f, (float)d);
    }

    [Fact]
    public void ExplicitToInt_Long_BigInteger_TruncateFraction()
    {
        var d = ClickHouseDecimal.Parse("123.99");
        Assert.Equal(123, (int)d);
        Assert.Equal(123L, (long)d);
        Assert.Equal(new BigInteger(123), (BigInteger)d);
    }

    [Fact]
    public void ExplicitToInt_OutOfRange_ThrowsOverflow()
    {
        // The XML doc says "wraps", but the cast delegates to BigInteger->int which throws.
        var d = ClickHouseDecimal.Parse("4294967296"); // 2^32
        Assert.Throws<OverflowException>(() => (int)d);
    }

    [Fact]
    public void ExplicitToBigInteger_PreservesLargeIntegerPart()
    {
        var big = new string('7', 60);
        Assert.Equal(BigInteger.Parse(big), (BigInteger)ClickHouseDecimal.Parse($"{big}.5"));
    }

    // ---- IConvertible ----------------------------------------------------------

    [Fact]
    public void IConvertible_GetTypeCode_IsObject() =>
        Assert.Equal(TypeCode.Object, ((IConvertible)ClickHouseDecimal.One).GetTypeCode());

    [Fact]
    public void IConvertible_ToType_SupportedTargets()
    {
        var d = ClickHouseDecimal.Parse("42.5");
        var c = (IConvertible)d;
        Assert.Equal(42.5m, c.ToType(typeof(decimal), null));
        Assert.Equal(42.5d, c.ToType(typeof(double), null));
        Assert.Equal(42, c.ToType(typeof(int), null));
        Assert.Equal(42L, c.ToType(typeof(long), null));
        Assert.Equal(new BigInteger(42), c.ToType(typeof(BigInteger), null));
        Assert.Equal("42.5", c.ToType(typeof(string), null));
        Assert.Equal(true, c.ToType(typeof(bool), null));
        Assert.Same(typeof(ClickHouseDecimal), c.ToType(typeof(ClickHouseDecimal), null).GetType());
    }

    [Fact]
    public void IConvertible_ToBoolean_ReflectsMantissa()
    {
        Assert.False(((IConvertible)ClickHouseDecimal.Zero).ToBoolean(null));
        Assert.True(((IConvertible)ClickHouseDecimal.One).ToBoolean(null));
    }

    [Fact]
    public void IConvertible_ToChar_And_ToDateTime_Throw()
    {
        var c = (IConvertible)ClickHouseDecimal.One;
        Assert.Throws<InvalidCastException>(() => c.ToChar(null));
        Assert.Throws<InvalidCastException>(() => c.ToDateTime(null));
    }

    [Fact]
    public void IConvertible_ToType_Unsupported_Throws() =>
        Assert.Throws<InvalidCastException>(() => ((IConvertible)ClickHouseDecimal.One).ToType(typeof(Guid), null));
}
