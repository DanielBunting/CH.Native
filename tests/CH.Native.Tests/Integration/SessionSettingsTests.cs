using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Custom ClickHouse settings applied via a session-scoped <c>SET</c> statement persist for subsequent
/// queries on the same open connection (the reachable-today slice of per-query settings — CH.Native has
/// no public custom-settings API). Exercises the session-state path that <c>TempTableLifetimeTests</c>
/// also relies on.
/// </summary>
[Collection("ClickHouse")]
public class SessionSettingsTests
{
    private readonly ClickHouseFixture _fixture;

    public SessionSettingsTests(ClickHouseFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task SetStatement_AppliesToNextQuery()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync("SET max_block_size = 12345");

        var value = await connection.ExecuteScalarAsync<string>("SELECT getSetting('max_block_size')");
        Assert.Equal("12345", value);
    }

    [Fact]
    public async Task SetStatement_PersistsAcrossMultipleQueries()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync("SET max_threads = 3");

        // An unrelated query in between must not clear the session setting.
        _ = await connection.ExecuteScalarAsync<long>("SELECT count() FROM numbers(10)");

        var value = await connection.ExecuteScalarAsync<string>("SELECT getSetting('max_threads')");
        Assert.Equal("3", value);
    }

    [Fact]
    public async Task MultipleSettings_InOneStatement_AllApply()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync("SET max_block_size = 4096, max_threads = 2");

        var blockSize = await connection.ExecuteScalarAsync<string>("SELECT getSetting('max_block_size')");
        var threads = await connection.ExecuteScalarAsync<string>("SELECT getSetting('max_threads')");
        Assert.Equal("4096", blockSize);
        Assert.Equal("2", threads);
    }

    [Fact]
    public async Task SettingChangesQueryBehavior()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // The `limit` setting caps rows in the top-level result of a SELECT without an explicit LIMIT.
        await connection.ExecuteNonQueryAsync("SET limit = 3");

        await using var reader = await connection.ExecuteReaderAsync("SELECT number FROM numbers(100)");
        var rows = 0;
        while (await reader.ReadAsync())
            rows++;
        Assert.Equal(3, rows);
    }
}
