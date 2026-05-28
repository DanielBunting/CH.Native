using System.Data;
using CH.Native.Ado;
using CH.Native.Dapper;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration.Dapper;

/// <summary>
/// Tests for <see cref="IDbConnectionDapperExtensions"/> (B2): the
/// namespace-swap drop-in. Verifies the fast path fires for
/// <see cref="ClickHouseDbConnection"/> when typed as <see cref="IDbConnection"/>,
/// and that the route-to-Dapper fallback is wired correctly. This file
/// deliberately does NOT import the <c>Dapper</c> namespace — to mirror the
/// user pattern of replacing <c>using Dapper;</c> with <c>using CH.Native.Dapper;</c>.
/// </summary>
[Collection("ClickHouse")]
public class IDbConnectionDapperExtensionsTests
{
    private readonly ClickHouseFixture _fixture;

    public IDbConnectionDapperExtensionsTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    // ------------------------------------------------------------------
    // Fast path through IDbConnection-typed variable when the connection
    // is actually a ClickHouseDbConnection.
    // ------------------------------------------------------------------

    [Fact]
    public async Task QueryAsync_OnCh_TypedAsIDbConnection_FastPathReturnsRows()
    {
        await using var ch = new ClickHouseDbConnection(_fixture.ConnectionString);
        await ch.OpenAsync();
        IDbConnection conn = ch;

        var rows = await conn.QueryAsync<NumberRow>(
            "SELECT toInt64(number) AS Value FROM numbers(3) ORDER BY number");
        Assert.Equal(new[] { 0L, 1L, 2L }, rows.Select(r => r.Value));
    }

    [Fact]
    public async Task QueryFirstAsync_OnCh_TypedAsIDbConnection_Works()
    {
        await using var ch = new ClickHouseDbConnection(_fixture.ConnectionString);
        await ch.OpenAsync();
        IDbConnection conn = ch;

        var r = await conn.QueryFirstAsync<NumberRow>("SELECT toInt64(42) AS Value");
        Assert.Equal(42L, r.Value);
    }

    [Fact]
    public async Task QueryFirstOrDefaultAsync_OnCh_EmptyResult_ReturnsNull()
    {
        await using var ch = new ClickHouseDbConnection(_fixture.ConnectionString);
        await ch.OpenAsync();
        IDbConnection conn = ch;

        var r = await conn.QueryFirstOrDefaultAsync<NumberRow>(
            "SELECT toInt64(0) AS Value WHERE 1 = 0");
        Assert.Null(r);
    }

    [Fact]
    public async Task QuerySingleAsync_OnCh_MultipleRows_Throws()
    {
        await using var ch = new ClickHouseDbConnection(_fixture.ConnectionString);
        await ch.OpenAsync();
        IDbConnection conn = ch;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            conn.QuerySingleAsync<NumberRow>(
                "SELECT toInt64(number) AS Value FROM numbers(3)"));
    }

    // ------------------------------------------------------------------
    // Parameter binding via the fast path.
    // ------------------------------------------------------------------

    [Fact]
    public async Task QueryAsync_OnCh_WithAnonymousParams_BindsCorrectly()
    {
        await using var ch = new ClickHouseDbConnection(_fixture.ConnectionString);
        await ch.OpenAsync();
        IDbConnection conn = ch;

        var rows = await conn.QueryAsync<NumberRow>(
            "SELECT toInt64(@x) AS Value", new { x = 7 });
        Assert.Equal(7L, rows.Single().Value);
    }

    // ------------------------------------------------------------------
    // commandTimeout forces Dapper fallback path — verifies the fast-path
    // gate respects the parameter rather than silently dropping it.
    // ------------------------------------------------------------------

    [Fact]
    public async Task QueryAsync_WithCommandTimeout_DelegatesToDapper()
    {
        // commandTimeout != null forces the Dapper delegation branch. Dapper
        // sets DbCommand.CommandTimeout, which our ADO.NET layer supports,
        // so the query succeeds — but it travels Dapper's mapper, not our
        // fast path. This test just confirms the call doesn't fail when
        // commandTimeout is supplied.
        await using var ch = new ClickHouseDbConnection(_fixture.ConnectionString);
        await ch.OpenAsync();
        IDbConnection conn = ch;

        var rows = await conn.QueryAsync<NumberRow>(
            "SELECT toInt64(1) AS Value",
            commandTimeout: 30);
        Assert.Single(rows);
        Assert.Equal(1L, rows.Single().Value);
    }

    // ------------------------------------------------------------------
    // ExecuteAsync — always delegates to Dapper. Verify it works against
    // a CH connection end-to-end (Dapper → ClickHouseDbCommand →
    // native execute).
    // ------------------------------------------------------------------

    [Fact]
    public async Task ExecuteAsync_DelegatesToDapper_AndWorksAgainstCh()
    {
        var tableName = $"_dapper_b2_{Guid.NewGuid():N}";
        await using var ch = new ClickHouseDbConnection(_fixture.ConnectionString);
        await ch.OpenAsync();
        IDbConnection conn = ch;

        try
        {
            await conn.ExecuteAsync(
                $"CREATE TABLE {tableName} (id Int32) ENGINE = Memory");
            await conn.ExecuteAsync(
                $"INSERT INTO {tableName} VALUES (1), (2), (3)");

            var count = await conn.QueryFirstAsync<NumberRow>(
                $"SELECT toInt64(count()) AS Value FROM {tableName}");
            Assert.Equal(3L, count.Value);
        }
        finally
        {
            await conn.ExecuteAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    public class NumberRow { public long Value { get; set; } }
}
