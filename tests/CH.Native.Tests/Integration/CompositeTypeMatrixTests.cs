using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Breadth matrix that exercises base decoders inside composite contexts (Array / Nullable / Tuple /
/// Map / Variant), targeting the decoder×context combinations the hand-picked smoke tests cover
/// unevenly (e.g. Map(String, IPv6), Tuple(Decimal32, String), Variant(UUID, ...)). Ported from the
/// driver's GenerateCompositeTypeSamples idea. Round-trips a server-side literal and asserts it
/// materializes to a non-null value without a decode error.
/// </summary>
[Collection("ClickHouse")]
public class CompositeTypeMatrixTests
{
    private readonly ClickHouseFixture _fixture;

    public CompositeTypeMatrixTests(ClickHouseFixture fixture) => _fixture = fixture;

    public static IEnumerable<object[]> CompositeExpressions()
    {
        // expression, expectAllowExperimental
        yield return new object[] { "[1, NULL, 3]::Array(Nullable(Int32))", false };
        yield return new object[] { "[toIPv4('1.2.3.4'), toIPv4('5.6.7.8')]::Array(IPv4)", false };
        yield return new object[] { "map('a', toIPv6('::1'), 'b', toIPv6('::2'))", false };
        yield return new object[] { "map('k', toDecimal64(1.5, 2))", false };
        yield return new object[] { "tuple(toDecimal32(1.5, 2), 'x')", false };
        yield return new object[] { "tuple(generateUUIDv4(), toIPv4('9.9.9.9'))", false };
        yield return new object[] { "[('a', 1), ('b', 2)]::Array(Tuple(String, Int32))", false };
        yield return new object[] { "arrayMap(x -> toDecimal128(x, 3), [1, 2, 3])", false };
        yield return new object[] { "['x', NULL]::Array(Nullable(String))", false };
        yield return new object[] { "map('u', generateUUIDv4())", false };
        yield return new object[] { "1::Variant(UInt64, String)", true };
        yield return new object[] { "'txt'::Variant(UInt64, String)", true };
    }

    [Theory]
    [MemberData(nameof(CompositeExpressions))]
    public async Task CompositeLiteral_Materializes(string expression, bool experimental)
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (experimental)
        {
            // Variant is behind an experimental flag on some server versions.
            try
            {
                await connection.ExecuteNonQueryAsync("SET allow_experimental_variant_type = 1");
            }
            catch
            {
                return; // server doesn't support the setting/type — nothing to assert.
            }
        }

        await using var reader = await connection.ExecuteReaderAsync($"SELECT {expression} AS v");
        Assert.True(await reader.ReadAsync());
        Assert.False(reader.IsDBNull(0));
        Assert.NotNull(reader.GetValue(0));
    }
}
