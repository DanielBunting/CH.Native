using CH.Native.Connection;

namespace CH.Native.SystemTests.Security.Helpers;

/// <summary>
/// Per-test (id Int32, value String) MergeTree harness for the Security suite.
/// Mirrors BulkInsertTableHarness but exposes parameterised insert / read helpers
/// so the round-trip tests don't have to repeat the boilerplate per case.
/// </summary>
internal sealed class EscapeTableHarness : IAsyncDisposable
{
    private readonly Func<ClickHouseConnectionSettings> _settingsFactory;

    public string TableName { get; }

    private EscapeTableHarness(string tableName, Func<ClickHouseConnectionSettings> settingsFactory)
    {
        TableName = tableName;
        _settingsFactory = settingsFactory;
    }

    public static async Task<EscapeTableHarness> CreateAsync(
        Func<ClickHouseConnectionSettings> settingsFactory,
        string namePrefix = "esc_test")
    {
        var name = $"{namePrefix}_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(settingsFactory());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {name} (id Int32, value String) ENGINE = MergeTree ORDER BY id");
        return new EscapeTableHarness(name, settingsFactory);
    }

    public async Task InsertAsync(int id, string value)
    {
        await using var conn = new ClickHouseConnection(_settingsFactory());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"INSERT INTO {TableName} (id, value) VALUES (@id, @v)",
            new { id, v = value });
    }

    public async Task<string> ReadValueAsync(int id)
    {
        await using var conn = new ClickHouseConnection(_settingsFactory());
        await conn.OpenAsync();
        var result = await conn.ExecuteScalarAsync<string>(
            $"SELECT value FROM {TableName} WHERE id = @id",
            new { id });
        return result ?? string.Empty;
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await using var conn = new ClickHouseConnection(_settingsFactory());
            await conn.OpenAsync();
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {TableName}");
        }
        catch
        {
            // Best-effort teardown; unique-named tables won't collide on next run.
        }
    }
}

/// <summary>
/// Sentinel table that hostile-payload tests assert still exists after a malicious
/// query value is round-tripped. Each test class creates one in IAsyncLifetime.
/// </summary>
internal sealed class SentinelTable : IAsyncDisposable
{
    private readonly Func<ClickHouseConnectionSettings> _settingsFactory;

    public string TableName { get; }

    private SentinelTable(string tableName, Func<ClickHouseConnectionSettings> settingsFactory)
    {
        TableName = tableName;
        _settingsFactory = settingsFactory;
    }

    public static async Task<SentinelTable> CreateAsync(
        Func<ClickHouseConnectionSettings> settingsFactory,
        string namePrefix = "sentinel_must_survive")
    {
        var name = $"{namePrefix}_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(settingsFactory());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {name} (id Int32) ENGINE = MergeTree ORDER BY id");
        return new SentinelTable(name, settingsFactory);
    }

    public async Task<bool> ExistsAsync()
    {
        await using var conn = new ClickHouseConnection(_settingsFactory());
        await conn.OpenAsync();
        var count = await conn.ExecuteScalarAsync<ulong>(
            "SELECT count() FROM system.tables WHERE name = @n",
            new { n = TableName });
        return count > 0;
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await using var conn = new ClickHouseConnection(_settingsFactory());
            await conn.OpenAsync();
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {TableName}");
        }
        catch
        {
            // Best-effort teardown.
        }
    }
}
