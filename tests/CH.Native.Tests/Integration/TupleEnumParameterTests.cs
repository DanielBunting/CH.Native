using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// End-to-end coverage for Tuple and .NET-enum query parameters, enabled by the new
/// <see cref="Parameters.ClickHouseTypeMapper"/> inference and <see cref="Parameters.ParameterSerializer"/>
/// serialization. (Ported from the driver's tuple-parameter and Enum8 tests.)
/// </summary>
[Collection("ClickHouse")]
public class TupleEnumParameterTests
{
    private readonly ClickHouseFixture _fixture;

    public TupleEnumParameterTests(ClickHouseFixture fixture) => _fixture = fixture;

    private enum Color { Red = 0, Green = 1, Blue = 2 }

    [Fact]
    public async Task TupleParameter_ElementsRoundTrip()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var first = await connection.ExecuteScalarAsync<int>(
            "SELECT tupleElement(@t, 1)", new { t = (1, "a") });
        var second = await connection.ExecuteScalarAsync<string>(
            "SELECT tupleElement(@t, 2)", new { t = (1, "a") });

        Assert.Equal(1, first);
        Assert.Equal("a", second);
    }

    [Fact]
    public async Task EnumParameter_RoundTripsAsLabel()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Inferred type Enum8('Red'=0,'Green'=1,'Blue'=2); value bound as the 'Green' label.
        var result = await connection.ExecuteScalarAsync<string>(
            "SELECT toString(@e)", new { e = Color.Green });

        Assert.Equal("Green", result);
    }
}
