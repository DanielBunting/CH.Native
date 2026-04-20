using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class BulkInsertSchemaCacheTests
{
    private readonly ClickHouseFixture _fixture;

    public BulkInsertSchemaCacheTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private static readonly BulkInsertOptions CachingEnabled = new() { UseSchemaCache = true };

    [Fact]
    public async Task ConnectionSettings_UseSchemaCacheTrue_EnablesCacheByDefault()
    {
        var tableName = $"test_cache_{Guid.NewGuid():N}";
        var settings = ClickHouseConnectionSettings.Parse(_fixture.ConnectionString + ";SchemaCache=true");
        Assert.True(settings.UseSchemaCache);

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Name String) ENGINE = Memory");

        try
        {
            // No BulkInsertOptions passed — inserter should inherit UseSchemaCache from the connection.
            await using var inserter = connection.CreateBulkInserter<CacheRow>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new CacheRow { Id = 1, Name = "one" });
            await inserter.CompleteAsync();

            Assert.Equal(1, connection.SchemaCache.Count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task PerCallOption_False_OverridesConnectionDefaultTrue()
    {
        var tableName = $"test_cache_{Guid.NewGuid():N}";
        var settings = new ClickHouseConnectionSettingsBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithUsername(_fixture.Username)
            .WithPassword(_fixture.Password)
            .WithSchemaCache(true)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Name String) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { UseSchemaCache = false };
            await using var inserter = connection.CreateBulkInserter<CacheRow>(tableName, options);
            await inserter.InitAsync();
            await inserter.AddAsync(new CacheRow { Id = 1, Name = "one" });
            await inserter.CompleteAsync();

            Assert.Equal(0, connection.SchemaCache.Count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task WithSchemaCache_FirstInserter_PopulatesCache()
    {
        var tableName = $"test_cache_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Name String) ENGINE = Memory");

        try
        {
            Assert.Equal(0, connection.SchemaCache.Count);

            await using var inserter = connection.CreateBulkInserter<CacheRow>(tableName, CachingEnabled);
            await inserter.InitAsync();
            await inserter.AddAsync(new CacheRow { Id = 1, Name = "one" });
            await inserter.CompleteAsync();

            Assert.Equal(1, connection.SchemaCache.Count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task WithSchemaCache_RepeatedInserters_ReuseCacheEntry()
    {
        var tableName = $"test_cache_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Name String) ENGINE = Memory");

        try
        {
            for (int run = 0; run < 3; run++)
            {
                await using var inserter = connection.CreateBulkInserter<CacheRow>(tableName, CachingEnabled);
                await inserter.InitAsync();
                await inserter.AddAsync(new CacheRow { Id = run, Name = $"run_{run}" });
                await inserter.CompleteAsync();
            }

            Assert.Equal(1, connection.SchemaCache.Count);
            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(3, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task WithSchemaCache_DifferentColumnSubsets_DoNotAlias()
    {
        // Regression test: key must include column-list fingerprint, not table name alone.
        var tableName = $"test_cache_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Name String, Extra String) ENGINE = Memory");

        try
        {
            await using (var fullInserter = connection.CreateBulkInserter<FullRow>(tableName, CachingEnabled))
            {
                await fullInserter.InitAsync();
                await fullInserter.AddAsync(new FullRow { Id = 1, Name = "n", Extra = "e" });
                await fullInserter.CompleteAsync();
            }

            await using (var partialInserter = connection.CreateBulkInserter<PartialRow>(tableName, CachingEnabled))
            {
                await partialInserter.InitAsync();
                await partialInserter.AddAsync(new PartialRow { Id = 2, Name = "n2" });
                await partialInserter.CompleteAsync();
            }

            // Two distinct fingerprints → two cache entries
            Assert.Equal(2, connection.SchemaCache.Count);
            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task UseSchemaCacheFalse_DoesNotPopulateCache()
    {
        var tableName = $"test_cache_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Name String) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<CacheRow>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new CacheRow { Id = 1, Name = "one" });
            await inserter.CompleteAsync();

            Assert.Equal(0, connection.SchemaCache.Count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task InvalidateSchemaCache_ByTable_RemovesEntriesForThatTable()
    {
        var tableA = $"test_cache_{Guid.NewGuid():N}";
        var tableB = $"test_cache_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableA} (Id Int32, Name String) ENGINE = Memory");
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableB} (Id Int32, Name String) ENGINE = Memory");

        try
        {
            await connection.BulkInsertAsync(tableA, new[] { new CacheRow { Id = 1, Name = "a" } }, CachingEnabled);
            await connection.BulkInsertAsync(tableB, new[] { new CacheRow { Id = 2, Name = "b" } }, CachingEnabled);

            Assert.Equal(2, connection.SchemaCache.Count);

            connection.InvalidateSchemaCache(tableA);

            Assert.Equal(1, connection.SchemaCache.Count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableA}");
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableB}");
        }
    }

    [Fact]
    public async Task InvalidateSchemaCache_WithNullArg_ClearsAll()
    {
        var tableName = $"test_cache_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Name String) ENGINE = Memory");

        try
        {
            await connection.BulkInsertAsync(tableName, new[] { new CacheRow { Id = 1, Name = "a" } }, CachingEnabled);
            Assert.Equal(1, connection.SchemaCache.Count);

            connection.InvalidateSchemaCache();

            Assert.Equal(0, connection.SchemaCache.Count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task CacheCleared_OnConnectionClose()
    {
        var tableName = $"test_cache_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Name String) ENGINE = Memory");

        try
        {
            await connection.BulkInsertAsync(tableName, new[] { new CacheRow { Id = 1, Name = "a" } }, CachingEnabled);
            Assert.Equal(1, connection.SchemaCache.Count);

            await connection.CloseAsync();

            Assert.Equal(0, connection.SchemaCache.Count);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task SchemaDrift_AfterDropColumn_EvictsCacheAndSurfacesError()
    {
        var tableName = $"test_cache_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Name String) ENGINE = Memory");

        try
        {
            // Prime the cache.
            await connection.BulkInsertAsync(tableName, new[] { new CacheRow { Id = 1, Name = "a" } }, CachingEnabled);
            Assert.Equal(1, connection.SchemaCache.Count);

            // Server-side drift: drop the Name column.
            await connection.ExecuteNonQueryAsync($"ALTER TABLE {tableName} DROP COLUMN Name");

            // The cached schema still references Name; next insert should fail, and the
            // cache entry should be evicted.
            await Assert.ThrowsAsync<ClickHouseServerException>(() =>
                connection.BulkInsertAsync(tableName, new[] { new CacheRow { Id = 2, Name = "b" } }, CachingEnabled));

            Assert.Equal(0, connection.SchemaCache.Count);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #region Row types

    private class CacheRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class FullRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Extra { get; set; } = string.Empty;
    }

    private class PartialRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    #endregion
}
