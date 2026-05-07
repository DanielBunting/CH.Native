using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class QualifiedTableNameTests
{
    private readonly ClickHouseFixture _fixture;

    public QualifiedTableNameTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private sealed class IdRow
    {
        public int Id { get; set; }
    }

    [Fact]
    public async Task QualifiedTableName_RoutesToCorrectDatabase()
    {
        var dbA = $"qbulk_a_{Guid.NewGuid():N}";
        var dbB = $"qbulk_b_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($"CREATE DATABASE {dbA}");
        await connection.ExecuteNonQueryAsync($"CREATE DATABASE {dbB}");

        try
        {
            await connection.ExecuteNonQueryAsync($"CREATE TABLE {dbA}.t (Id Int32) ENGINE = Memory");
            await connection.ExecuteNonQueryAsync($"CREATE TABLE {dbB}.t (Id Int32) ENGINE = Memory");

            await connection.BulkInsertAsync<IdRow>(
                $"{dbA}.t",
                new[] { new IdRow { Id = 1 }, new IdRow { Id = 2 } });
            await connection.BulkInsertAsync<IdRow>(
                $"{dbB}.t",
                new[] { new IdRow { Id = 100 }, new IdRow { Id = 200 }, new IdRow { Id = 300 } });

            Assert.Equal(2, await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {dbA}.t"));
            Assert.Equal(3, await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {dbB}.t"));
            Assert.Equal(3, await connection.ExecuteScalarAsync<long>($"SELECT count(DISTINCT Id) FROM {dbB}.t"));
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP DATABASE IF EXISTS {dbA}");
            await connection.ExecuteNonQueryAsync($"DROP DATABASE IF EXISTS {dbB}");
        }
    }

    [Fact]
    public async Task ExplicitDatabaseTableOverload_RoutesIdentically()
    {
        var dbB = $"qbulk_explicit_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($"CREATE DATABASE {dbB}");
        try
        {
            await connection.ExecuteNonQueryAsync($"CREATE TABLE {dbB}.t (Id Int32) ENGINE = Memory");

            await connection.BulkInsertAsync<IdRow>(
                database: dbB,
                tableName: "t",
                rows: new[] { new IdRow { Id = 7 } });

            Assert.Equal(1, await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {dbB}.t"));
            Assert.Equal(7, await connection.ExecuteScalarAsync<int>($"SELECT Id FROM {dbB}.t"));
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP DATABASE IF EXISTS {dbB}");
        }
    }

    [Fact]
    public async Task UnknownDatabase_QualifiedName_RaisesServerException()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // The 'does_not_exist' DB name is unique enough not to collide with a real DB.
        var ex = await Assert.ThrowsAnyAsync<ClickHouseServerException>(async () =>
        {
            await connection.BulkInsertAsync<IdRow>(
                $"does_not_exist_{Guid.NewGuid():N}.t",
                new[] { new IdRow { Id = 1 } });
        });

        // The connection should still be usable after the server-side rejection.
        Assert.Equal(1, await connection.ExecuteScalarAsync<int>("SELECT 1"));
        Assert.NotNull(ex);
    }

    [Fact]
    public async Task SchemaCache_KeyedByDatabase_NoCrossContamination()
    {
        var dbA = $"qbulk_cache_a_{Guid.NewGuid():N}";
        var dbB = $"qbulk_cache_b_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($"CREATE DATABASE {dbA}");
        await connection.ExecuteNonQueryAsync($"CREATE DATABASE {dbB}");

        try
        {
            // Two databases, each with a same-named table — the cache key must
            // include the database to prevent cross-DB collisions on the same
            // long-lived connection.
            await connection.ExecuteNonQueryAsync($"CREATE TABLE {dbA}.events (Id Int32) ENGINE = Memory");
            await connection.ExecuteNonQueryAsync($"CREATE TABLE {dbB}.events (Id Int32) ENGINE = Memory");

            await connection.BulkInsertAsync<IdRow>(
                $"{dbA}.events",
                new[] { new IdRow { Id = 1 } });
            await connection.BulkInsertAsync<IdRow>(
                $"{dbB}.events",
                new[] { new IdRow { Id = 2 } });

            Assert.Equal(1, await connection.ExecuteScalarAsync<int>($"SELECT Id FROM {dbA}.events"));
            Assert.Equal(2, await connection.ExecuteScalarAsync<int>($"SELECT Id FROM {dbB}.events"));
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP DATABASE IF EXISTS {dbA}");
            await connection.ExecuteNonQueryAsync($"DROP DATABASE IF EXISTS {dbB}");
        }
    }

    [Fact]
    public async Task InvalidateSchemaCache_QualifiedAndExplicit_BothEvict()
    {
        var dbA = $"qbulk_inv_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($"CREATE DATABASE {dbA}");
        try
        {
            await connection.ExecuteNonQueryAsync($"CREATE TABLE {dbA}.t (Id Int32) ENGINE = Memory");

            // Populate cache via an insert.
            await connection.BulkInsertAsync<IdRow>($"{dbA}.t", new[] { new IdRow { Id = 1 } });

            // Both the qualified-string and explicit-overload forms should resolve to
            // the same key and evict it.
            connection.InvalidateSchemaCache($"{dbA}.t");
            connection.InvalidateSchemaCache(dbA, "t");

            // No exception, no error — the actual eviction is tested at the unit level via
            // SchemaCache. Here we just pin that the public InvalidateSchemaCache surface
            // still accepts the qualified form.
            Assert.Equal(1, await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {dbA}.t"));
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP DATABASE IF EXISTS {dbA}");
        }
    }
}
