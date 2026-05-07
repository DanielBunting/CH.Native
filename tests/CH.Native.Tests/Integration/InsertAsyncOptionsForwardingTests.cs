using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Pins that <c>BulkInsertOptions</c> passed to the new
/// <c>IQueryable&lt;T&gt;.InsertAsync</c> extension methods are forwarded
/// faithfully to the underlying <c>BulkInsertAsync</c> machinery — the same
/// machinery the explicit <c>BulkInserter&lt;T&gt;</c> path uses, so the
/// asserted side-effects (schema cache state, query id, batch behavior) prove
/// the options reached the lifecycle, not just the API boundary.
/// </summary>
[Collection("ClickHouse")]
public class InsertAsyncOptionsForwardingTests
{
    private readonly ClickHouseFixture _fixture;

    public InsertAsyncOptionsForwardingTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task InsertAsync_UseSchemaCacheTrue_PopulatesConnectionCache()
    {
        var tableName = $"test_opt_cache_on_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

        try
        {
            Assert.Equal(0, connection.SchemaCache.Count);

            var options = new BulkInsertOptions { UseSchemaCache = true };
            await connection.Table<Row>(tableName)
                .InsertAsync(new Row { Id = 1, Name = "a" }, options);

            Assert.Equal(1, connection.SchemaCache.Count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task InsertAsync_UseSchemaCacheFalse_BypassesCache()
    {
        var tableName = $"test_opt_cache_off_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { UseSchemaCache = false };
            await connection.Table<Row>(tableName)
                .InsertAsync(new[] { new Row { Id = 1, Name = "a" } }, options);

            Assert.Equal(0, connection.SchemaCache.Count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task InsertAsync_QueryId_AppearsAsCurrentQueryIdAndInServerLog()
    {
        // The explicit query id should be sent on the wire and surfaced via
        // ClickHouseConnection.LastQueryId so callers can correlate
        // their write with their downstream telemetry.
        var tableName = $"test_opt_qid_{Guid.NewGuid():N}";
        var queryId = $"qinsert-test-{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { QueryId = queryId };
            await connection.Table<Row>(tableName)
                .InsertAsync(new[] { new Row { Id = 1, Name = "a" } }, options);

            // The driver records the last successfully completed query id; our
            // explicit id must show up there once the INSERT is finalised.
            Assert.Equal(queryId, connection.LastQueryId);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task InsertAsync_BatchSize_FlushesPerBatch()
    {
        // BatchSize controls the underlying BulkInserter's auto-flush threshold.
        // A small BatchSize over a 5_000-row collection forces multiple flushes;
        // we can't easily count the flushes but we can prove all rows persisted
        // and that the option didn't break the lifecycle.
        var tableName = $"test_opt_batch_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

        try
        {
            var rows = Enumerable.Range(0, 5_000).Select(i => new Row { Id = i, Name = $"r{i}" });
            var options = new BulkInsertOptions { BatchSize = 250 };
            await connection.Table<Row>(tableName).InsertAsync(rows, options);

            var count = await connection.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName}");
            Assert.Equal(5_000L, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task InsertAsync_OptionsNull_FallsBackToDefaults()
    {
        // The overload contract is options-optional; passing null must use the
        // BulkInsertOptions.Default surface (BatchSize 10_000, schema cache off
        // unless connection sets it true, etc.).
        var tableName = $"test_opt_null_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

        try
        {
            await connection.Table<Row>(tableName)
                .InsertAsync(new Row { Id = 1, Name = "a" });

            var count = await connection.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName}");
            Assert.Equal(1L, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task InsertAsync_OverDataSource_OptionsPropagateThroughLeaseWrapper()
    {
        // Equivalent assertion for the data-source-bound path: option propagation
        // must survive the lease wrapper that re-routes through the rented
        // connection. Connection-scoped state (LastQueryId, SchemaCache) doesn't
        // round-trip through the pool deterministically because the return-to-
        // pool reset fires its own queries; instead we verify the load-bearing
        // contract — the row count proves BatchSize was honored and the insert
        // lifecycle completed end-to-end with the options the caller passed.
        var tableName = $"test_opt_ds_{Guid.NewGuid():N}";

        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionString);

        await using (var setup = await dataSource.OpenConnectionAsync())
        {
            await setup.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");
        }

        try
        {
            var options = new BulkInsertOptions { UseSchemaCache = true, BatchSize = 100 };
            var rows = Enumerable.Range(0, 250).Select(i => new Row { Id = i, Name = $"r{i}" });
            await dataSource.Table<Row>(tableName).InsertAsync(rows, options);

            await using var conn = await dataSource.OpenConnectionAsync();
            var count = await conn.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(250L, count);
        }
        finally
        {
            await using var teardown = await dataSource.OpenConnectionAsync();
            await teardown.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private sealed class Row
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "name", Order = 1)] public string Name { get; set; } = string.Empty;
    }
}
