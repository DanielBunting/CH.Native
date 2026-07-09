using CH.Native.Connection;
using CH.Native.Dapper;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Covers the First/Single result semantics of the CH.Native.Dapper extension methods
/// (<c>QueryFirst(OrDefault)Async</c> / <c>QuerySingle(OrDefault)Async</c>): empty, exactly-one, and
/// more-than-one result sets, including the InvalidOperationException paths.
/// </summary>
[Collection("ClickHouse")]
public class DapperFirstSingleTests
{
    private readonly ClickHouseFixture _fixture;

    public DapperFirstSingleTests(ClickHouseFixture fixture) => _fixture = fixture;

    public sealed class IntRow
    {
        public int Value { get; set; }
    }

    // numbers(k) yields k rows with number 0..k-1; project as Int32 column "Value".
    private static string Sql(int k) => $"SELECT toInt32(number) AS Value FROM numbers({k})";

    private async Task<ClickHouseConnection> OpenAsync()
    {
        var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    [Fact]
    public async Task QueryFirst_ReturnsFirstRow()
    {
        await using var c = await OpenAsync();
        var row = await c.QueryFirstAsync<IntRow>(Sql(3));
        Assert.Equal(0, row.Value);
    }

    [Fact]
    public async Task QueryFirst_Empty_Throws()
    {
        await using var c = await OpenAsync();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => c.QueryFirstAsync<IntRow>(Sql(0)));
        Assert.Contains("no elements", ex.Message);
    }

    [Fact]
    public async Task QueryFirstOrDefault_ReturnsRow()
    {
        await using var c = await OpenAsync();
        var row = await c.QueryFirstOrDefaultAsync<IntRow>(Sql(2));
        Assert.NotNull(row);
        Assert.Equal(0, row!.Value);
    }

    [Fact]
    public async Task QueryFirstOrDefault_Empty_ReturnsDefault()
    {
        await using var c = await OpenAsync();
        var row = await c.QueryFirstOrDefaultAsync<IntRow>(Sql(0));
        Assert.Null(row);
    }

    [Fact]
    public async Task QuerySingle_ExactlyOne_Returns()
    {
        await using var c = await OpenAsync();
        var row = await c.QuerySingleAsync<IntRow>(Sql(1));
        Assert.Equal(0, row.Value);
    }

    [Fact]
    public async Task QuerySingle_Empty_Throws()
    {
        await using var c = await OpenAsync();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => c.QuerySingleAsync<IntRow>(Sql(0)));
        Assert.Contains("no elements", ex.Message);
    }

    [Fact]
    public async Task QuerySingle_MoreThanOne_Throws()
    {
        await using var c = await OpenAsync();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => c.QuerySingleAsync<IntRow>(Sql(2)));
        Assert.Contains("more than one", ex.Message);
    }

    [Fact]
    public async Task QuerySingleOrDefault_ExactlyOne_Returns()
    {
        await using var c = await OpenAsync();
        var row = await c.QuerySingleOrDefaultAsync<IntRow>(Sql(1));
        Assert.NotNull(row);
        Assert.Equal(0, row!.Value);
    }

    [Fact]
    public async Task QuerySingleOrDefault_Empty_ReturnsDefault()
    {
        await using var c = await OpenAsync();
        var row = await c.QuerySingleOrDefaultAsync<IntRow>(Sql(0));
        Assert.Null(row);
    }

    [Fact]
    public async Task QuerySingleOrDefault_MoreThanOne_Throws()
    {
        await using var c = await OpenAsync();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => c.QuerySingleOrDefaultAsync<IntRow>(Sql(2)));
        Assert.Contains("more than one", ex.Message);
    }
}
