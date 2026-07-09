using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// A <c>Variant(...)</c> value binds as a query parameter and the active alternative round-trips
/// (ported from the driver's Variant parameter coverage). The value's CLR type selects the variant
/// arm; <c>toString</c> on the server confirms the decoded value.
/// </summary>
[Collection("ClickHouse")]
public class VariantParameterTests
{
    private readonly ClickHouseFixture _fixture;

    public VariantParameterTests(ClickHouseFixture fixture) => _fixture = fixture;

    [Theory]
    [InlineData(42, "42")]
    [InlineData("hello", "hello")]
    public async Task VariantParameter_ActiveArm_RoundTrips(object value, string expectedText)
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        try { await connection.ExecuteNonQueryAsync("SET allow_experimental_variant_type = 1"); }
        catch { /* enabled by default on the pinned image; ignore if unsupported */ }

        await using var command = connection.CreateCommand(
            "SELECT toString({v:Variant(Int32, String)}) = {expected:String} ? 1 : 0");
        command.Parameters.Add("v", value, "Variant(Int32, String)");
        command.Parameters.Add("expected", expectedText, "String");

        var equal = await command.ExecuteScalarAsync<byte>();
        Assert.Equal((byte)1, equal);
    }
}
