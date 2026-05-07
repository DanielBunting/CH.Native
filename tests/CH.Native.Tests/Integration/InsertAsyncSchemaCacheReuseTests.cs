using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Pins schema-cache reuse across consecutive <c>InsertAsync</c> calls. The
/// cache is connection-scoped (<c>connection.SchemaCache</c>) and keyed by
/// <c>(database, table, columnList)</c>; the second insert into the same table
/// with the same column shape must reuse the entry rather than re-probe the
/// server. The <c>Count</c> stays at one because all inserts share a key.
/// </summary>
[Collection("ClickHouse")]
public class InsertAsyncSchemaCacheReuseTests
{
    private readonly ClickHouseFixture _fixture;

    public InsertAsyncSchemaCacheReuseTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ConsecutiveInserts_SameTable_KeepCacheCountAtOne()
    {
        var tableName = $"test_cache_reuse_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { UseSchemaCache = true };
            var table = connection.Table<Row>(tableName);

            for (var i = 0; i < 5; i++)
            {
                await table.InsertAsync(new Row { Id = i, Name = $"r{i}" }, options);
            }

            // Five inserts, one cached schema entry. If the cache were bypassed
            // or the key drifted, this would be 0 or >1.
            Assert.Equal(1, connection.SchemaCache.Count);

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(5L, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task DifferentTables_AddSeparateCacheEntries()
    {
        // Pin the cache key includes the table name — two distinct tables must
        // each populate their own entry, so the count rises.
        var tableA = $"test_cache_a_{Guid.NewGuid():N}";
        var tableB = $"test_cache_b_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync($"CREATE TABLE {tableA} (id Int32, name String) ENGINE = Memory");
        await connection.ExecuteNonQueryAsync($"CREATE TABLE {tableB} (id Int32, name String) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { UseSchemaCache = true };
            await connection.Table<Row>(tableA).InsertAsync(new Row { Id = 1, Name = "a" }, options);
            await connection.Table<Row>(tableB).InsertAsync(new Row { Id = 2, Name = "b" }, options);

            Assert.Equal(2, connection.SchemaCache.Count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableA}");
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableB}");
        }
    }

    [Fact]
    public async Task InvalidateSchemaCache_ForcesNextInsertToReProbe()
    {
        // Pin the manual eviction path: after a caller-driven invalidation, the
        // next insert must rebuild the entry — Count goes 1 → 0 → 1 across the
        // invalidate/insert pair.
        var tableName = $"test_cache_invalidate_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { UseSchemaCache = true };
            await connection.Table<Row>(tableName).InsertAsync(new Row { Id = 1, Name = "a" }, options);
            Assert.Equal(1, connection.SchemaCache.Count);

            connection.InvalidateSchemaCache();
            Assert.Equal(0, connection.SchemaCache.Count);

            await connection.Table<Row>(tableName).InsertAsync(new Row { Id = 2, Name = "b" }, options);
            Assert.Equal(1, connection.SchemaCache.Count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private sealed class Row
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "name", Order = 1)] public string Name { get; set; } = string.Empty;
    }
}
