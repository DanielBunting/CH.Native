using Apache.Arrow;
using Apache.Arrow.Adbc;
using CH.Native.Connection;
using Xunit;

namespace CH.Native.Adbc.Tests.Integration;

/// <summary>
/// Shared plumbing for ADBC integration tests: opens ADBC connections against the container,
/// runs setup DDL/DML via a raw CH.Native connection, and pulls a single RecordBatch through the
/// ADBC stream.
/// </summary>
public abstract class AdbcIntegrationTestBase
{
    protected AdbcClickHouseFixture Fixture { get; }

    protected AdbcIntegrationTestBase(AdbcClickHouseFixture fixture) => Fixture = fixture;

    /// <summary>Opens a fresh ADBC connection (its own driver/database/pool) for isolation.</summary>
    protected AdbcConnection OpenConnection()
    {
        var driver = new ClickHouseAdbcDriver();
        var database = driver.Open(new Dictionary<string, string>
        {
            [AdbcOptionKeys.ConnectionString] = Fixture.ConnectionString,
        });
        return database.Connect(null);
    }

    /// <summary>Runs setup statements (CREATE/INSERT) over a raw CH.Native connection.</summary>
    protected async Task ExecuteSetupAsync(params string[] statements)
    {
        await using var connection = new ClickHouseConnection(Fixture.ConnectionString);
        await connection.OpenAsync();
        foreach (var sql in statements)
            await connection.ExecuteNonQueryAsync(sql);
    }

    /// <summary>Opens a raw CH.Native connection (a <see cref="System.Data.Common.DbConnection"/>) for Dapper reads.</summary>
    protected async Task<ClickHouseConnection> OpenClickHouseAsync()
    {
        var connection = new ClickHouseConnection(Fixture.ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    /// <summary>
    /// Executes <paramref name="sql"/> and returns its first (and, for these single-block tests,
    /// only) non-empty RecordBatch. The stream is fully drained so the connection tears down cleanly.
    /// The returned batch owns Arrow buffers independent of the connection, so it outlives disposal.
    /// </summary>
    protected async Task<RecordBatch> QueryOneBatchAsync(string sql)
    {
        using var conn = OpenConnection();
        using var stmt = conn.CreateStatement();
        stmt.SqlQuery = sql;
        var result = stmt.ExecuteQuery();
        using var stream = result.Stream!;

        var batch = await stream.ReadNextRecordBatchAsync();
        Assert.NotNull(batch);

        // Exhaust so Dispose sees a completed stream (no spurious server cancel).
        while (await stream.ReadNextRecordBatchAsync() is { } extra)
            extra.Dispose();

        return batch!;
    }

    /// <summary>Asserts that <paramref name="sql"/> yields no data batch (empty result set).</summary>
    protected async Task AssertNoRowsAsync(string sql)
    {
        using var conn = OpenConnection();
        using var stmt = conn.CreateStatement();
        stmt.SqlQuery = sql;
        var result = stmt.ExecuteQuery();
        using var stream = result.Stream!;

        var batch = await stream.ReadNextRecordBatchAsync();
        Assert.Null(batch);
    }
}
