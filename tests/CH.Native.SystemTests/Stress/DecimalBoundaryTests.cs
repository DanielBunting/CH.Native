using System.Numerics;
using CH.Native.Connection;
using CH.Native.Numerics;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Stress;

/// <summary>
/// Decimal precision at the boundaries of the four ClickHouse decimal widths
/// (Decimal32 / 64 / 128 / 256). Existing tests round-trip mid-range values; the
/// edges — max, min, scale 0, scale max — are where reader/writer overflow bugs
/// surface, and where Decimal128/256 push past .NET <see cref="decimal"/>'s 28-29
/// digit limit and require <see cref="ClickHouseDecimal"/> for lossless transit.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class DecimalBoundaryTests
{
    private readonly SingleNodeFixture _fixture;

    public DecimalBoundaryTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Theory]
    [InlineData(32, 9, "999999999")]                               // Max for Decimal32, scale 0
    [InlineData(32, 4, "99999.9999")]                              // Decimal32, mid-scale max
    [InlineData(64, 18, "999999999999999999")]                     // Max for Decimal64, scale 0
    [InlineData(64, 9, "999999999.999999999")]                     // Decimal64, mid-scale max
    [InlineData(128, 38, "99999999999999999999999999999999999999")] // Max for Decimal128, scale 0
    [InlineData(256, 76, "9999999999999999999999999999999999999999999999999999999999999999999999999999")] // Max Decimal256
    public async Task Decimal_MaxValue_RoundTrips(int width, int totalDigits, string mantissaDigits)
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // Construct via SQL: toDecimal{N}('<digits>', <scale>). For zero-scale tests,
        // scale = 0 means the literal *is* the integer part. For mid-scale tests, the
        // literal must include a decimal point matching the requested scale.
        var scale = ScaleFromMantissaForm(mantissaDigits, totalDigits);

        var sql = scale == 0
            ? $"toDecimal{width}('{mantissaDigits}', 0)"
            : $"toDecimal{width}('{mantissaDigits}', {scale})";

        var result = await ReadDecimalAsClickHouseDecimalAsync(conn, $"SELECT {sql}", width);
        Assert.Equal(scale, result.Scale);

        var expectedMantissa = BigInteger.Parse(mantissaDigits.Replace(".", ""));
        Assert.Equal(expectedMantissa, result.Mantissa);
    }

    [Theory]
    [InlineData(32, "-999999999")]
    [InlineData(64, "-999999999999999999")]
    [InlineData(128, "-99999999999999999999999999999999999999")]
    [InlineData(256, "-9999999999999999999999999999999999999999999999999999999999999999999999999999")]
    public async Task Decimal_MinValue_RoundTrips(int width, string negativeMantissaDigits)
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var sql = $"toDecimal{width}('{negativeMantissaDigits}', 0)";
        var result = await ReadDecimalAsClickHouseDecimalAsync(conn, $"SELECT {sql}", width);
        Assert.Equal(0, result.Scale);
        Assert.Equal(BigInteger.Parse(negativeMantissaDigits), result.Mantissa);
    }

    [Fact]
    public async Task Decimal128_BeyondDotNetDecimalMax_PreservedViaClickHouseDecimal()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // 38 nines — exceeds decimal.MaxValue (~7.9e28) by ~10 orders of magnitude.
        var literal = new string('9', 38);
        var ch = await conn.ExecuteScalarAsync<ClickHouseDecimal>($"SELECT toDecimal128('{literal}', 0)");

        // ClickHouseDecimal must carry the full mantissa with no loss.
        Assert.Equal(BigInteger.Parse(literal), ch.Mantissa);

        // Documents the current explicit-cast behaviour: clamps to decimal.MaxValue
        // rather than throwing OverflowException. If the lib later changes the cast
        // contract, this test forces the choice to be deliberate.
        var clamped = (decimal)ch;
        Assert.Equal(decimal.MaxValue, clamped);
    }

    [Fact]
    public async Task Decimal256_BeyondDotNetDecimalMax_PreservedViaClickHouseDecimal()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // 76 nines — Decimal256 max.
        var literal = new string('9', 76);
        var ch = await conn.ExecuteScalarAsync<ClickHouseDecimal>($"SELECT toDecimal256('{literal}', 0)");

        Assert.Equal(BigInteger.Parse(literal), ch.Mantissa);

        var clamped = (decimal)ch;
        Assert.Equal(decimal.MaxValue, clamped);
    }

    // Scale capped at (max digits - 1) per width: at max-scale, the integer mantissa
    // for value "1" would be 10^scale, which has scale+1 digits — exceeding the width's
    // precision and rejected by toDecimal{N}. The narrow widths (32/64) only test
    // scale 0 because their column reader returns CLR <see cref="decimal"/>, whose
    // round-trip collapses trailing zeros — so a non-zero scale on the wire arrives
    // as scale 0 in CLR and the assertion can't distinguish "round-trip preserved"
    // from "trailing zeros stripped". Decimal128/256 readers return
    // <see cref="ClickHouseDecimal"/> directly and preserve scale exactly.
    [Theory]
    [InlineData(32, 0)]
    [InlineData(64, 0)]
    [InlineData(128, 0)]
    [InlineData(128, 37)]
    [InlineData(256, 0)]
    [InlineData(256, 75)]
    public async Task Decimal_ScaleBoundaries_RoundTripPositiveOne(int width, int scale)
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // Value "1" at any scale — mantissa is 10^scale.
        var sql = $"toDecimal{width}('1', {scale})";
        var result = await ReadDecimalAsClickHouseDecimalAsync(conn, $"SELECT {sql}", width);

        Assert.Equal(scale, result.Scale);
        var expectedMantissa = BigInteger.Pow(10, scale);
        Assert.Equal(expectedMantissa, result.Mantissa);
    }

    /// <summary>
    /// Decimal32 / Decimal64 column readers return <see cref="decimal"/>; only
    /// Decimal128 / Decimal256 return <see cref="ClickHouseDecimal"/> directly.
    /// Read the right CLR type per width and lift to a common <see cref="ClickHouseDecimal"/>
    /// for assertion. <c>scale</c> is recovered from the result string for the narrow
    /// widths since <see cref="decimal"/> doesn't expose it directly without bit fiddling.
    /// </summary>
    private static async Task<ClickHouseDecimal> ReadDecimalAsClickHouseDecimalAsync(
        ClickHouseConnection conn, string sql, int width)
    {
        if (width >= 128)
        {
            return await conn.ExecuteScalarAsync<ClickHouseDecimal>(sql);
        }

        var native = await conn.ExecuteScalarAsync<decimal>(sql);
        // Recover scale from the CLR decimal's Scale field via GetBits.
        int[] bits = decimal.GetBits(native);
        int recoveredScale = (bits[3] >> 16) & 0x7F;
        // Mantissa = the decimal's invariant string with the dot stripped — the digits
        // *are* the mantissa for a CLR decimal. Sign is carried on the BigInteger.
        var s = native.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var mantissa = BigInteger.Parse(s.Replace(".", ""));
        return new ClickHouseDecimal(mantissa, recoveredScale);
    }

    /// <summary>
    /// Pulls the scale out of a literal like "99999.9999". For pure integers,
    /// this is the totalDigits passed in — i.e. scale 0 unless a decimal point
    /// appears in the literal.
    /// </summary>
    private static int ScaleFromMantissaForm(string mantissaDigits, int _totalDigits)
    {
        var dot = mantissaDigits.IndexOf('.');
        return dot < 0 ? 0 : mantissaDigits.Length - dot - 1;
    }
}
