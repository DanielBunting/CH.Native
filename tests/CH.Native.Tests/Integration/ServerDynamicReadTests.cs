using CH.Native.Connection;
using CH.Native.Data.Dynamic;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Reading a server-produced <c>Dynamic</c> value (<c>x::T::Dynamic</c>) surfaces its declared type and
/// casts to the matching framework type (ported from the driver's Dynamic read/direct-cast matrix).
/// Complements <c>DynamicWriteTests</c>, which covers the write side.
/// </summary>
[Collection("ClickHouse")]
public class ServerDynamicReadTests
{
    private readonly ClickHouseFixture _fixture;

    public ServerDynamicReadTests(ClickHouseFixture fixture) => _fixture = fixture;

    private async Task<ClickHouseDynamic> ReadDynamicAsync(string typedExpr)
    {
        var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        try { await connection.ExecuteNonQueryAsync("SET allow_experimental_dynamic_type = 1"); } catch { }
        await using var reader = await connection.ExecuteReaderAsync($"SELECT {typedExpr}::Dynamic AS v");
        Assert.True(await reader.ReadAsync());
        var d = reader.GetFieldValue<ClickHouseDynamic>(0);
        await connection.DisposeAsync();
        return d;
    }

    [Theory]
    [InlineData("42::Int32", "Int32")]
    [InlineData("'hello'::String", "String")]
    [InlineData("3.5::Float64", "Float64")]
    [InlineData("true::Bool", "Bool")]
    [InlineData("toDate('2021-01-02')", "Date")]
    [InlineData("toUUID('11111111-2222-3333-4444-555555555555')", "UUID")]
    public async Task ServerDynamic_ExposesDeclaredType(string typedExpr, string expectedDeclaredType)
    {
        var d = await ReadDynamicAsync(typedExpr);
        Assert.False(d.IsNull);
        Assert.Equal(expectedDeclaredType, d.DeclaredTypeName);
    }

    [Fact]
    public async Task ServerDynamic_CastsToFrameworkTypes()
    {
        Assert.True((await ReadDynamicAsync("42::Int32")).TryGetAs<int>(out var i));
        Assert.Equal(42, i);

        Assert.True((await ReadDynamicAsync("'hello'::String")).TryGetAs<string>(out var s));
        Assert.Equal("hello", s);

        Assert.True((await ReadDynamicAsync("3.5::Float64")).TryGetAs<double>(out var f));
        Assert.Equal(3.5, f);

        Assert.True((await ReadDynamicAsync("true::Bool")).TryGetAs<bool>(out var b));
        Assert.True(b);

        Assert.True((await ReadDynamicAsync("toUUID('11111111-2222-3333-4444-555555555555')")).TryGetAs<Guid>(out var g));
        Assert.Equal(new Guid("11111111-2222-3333-4444-555555555555"), g);
    }
}
