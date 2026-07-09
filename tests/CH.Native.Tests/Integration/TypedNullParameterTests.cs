using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// A typed-null parameter round-trips to <see cref="DBNull"/> (ported from the driver's
/// ShouldExecuteSelectWithNullParameterWithoutExplicitType). Unlike the driver, CH.Native <b>requires</b>
/// an explicit <c>Nullable(T)</c> type on the parameter — binding a bare anonymous-object null throws in
/// type inference — so each case sets the type explicitly via <c>Parameters.Add(name, null, type)</c>.
/// </summary>
[Collection("ClickHouse")]
public class TypedNullParameterTests
{
    private readonly ClickHouseFixture _fixture;

    public TypedNullParameterTests(ClickHouseFixture fixture) => _fixture = fixture;

    [Theory]
    [InlineData("Nullable(String)")]
    [InlineData("Nullable(Int32)")]
    [InlineData("Nullable(Int64)")]
    [InlineData("Nullable(Float64)")]
    [InlineData("Nullable(UUID)")]
    [InlineData("Nullable(Date)")]
    [InlineData("Nullable(DateTime)")]
    [InlineData("Nullable(Bool)")]
    public async Task NullParameter_WithNullableType_RoundTripsToDbNull(string nullableType)
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand($"SELECT {{v:{nullableType}}} AS v");
        command.Parameters.Add("v", null, nullableType);

        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.True(reader.IsDBNull(0));
    }
}
