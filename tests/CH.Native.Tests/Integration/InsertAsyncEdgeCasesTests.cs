using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Edge cases that aren't covered by the round-trip suite: empty input
/// sequences, qualified <c>database.table</c> names, repeated calls on the
/// same handle. Each pin guards a real failure mode the new
/// <c>InsertAsync</c> surface introduced or could plausibly regress on.
/// </summary>
[Collection("ClickHouse")]
public class InsertAsyncEdgeCasesTests
{
    private readonly ClickHouseFixture _fixture;

    public InsertAsyncEdgeCasesTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task InsertAsync_EmptyEnumerable_LandsZeroRows_LeavesConnectionUsable()
    {
        var tableName = $"test_empty_enum_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

        try
        {
            await connection.Table<Row>(tableName).InsertAsync(Array.Empty<Row>());

            Assert.Equal(0L, await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}"));

            // Connection must remain usable for a follow-up insert.
            await connection.Table<Row>(tableName).InsertAsync(new Row { Id = 1, Name = "a" });
            Assert.Equal(1L, await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}"));
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task InsertAsync_EmptyAsyncEnumerable_LandsZeroRows_LeavesConnectionUsable()
    {
        var tableName = $"test_empty_async_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

        try
        {
            await connection.Table<Row>(tableName).InsertAsync(EmptyAsync());

            Assert.Equal(0L, await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}"));

            await connection.Table<Row>(tableName).InsertAsync(new Row { Id = 1, Name = "a" });
            Assert.Equal(1L, await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}"));
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }

        static async IAsyncEnumerable<Row> EmptyAsync()
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    [Fact]
    public async Task InsertAsync_QualifiedDatabaseTableName_RoutesToNamedDatabase()
    {
        // Pin: a "db.table" qualified name passed to Table<T>(...) must route the
        // INSERT to that database, even though the connection's default database
        // is whatever the connection string set.
        var dbName = $"sample_qualified_{Guid.NewGuid():N}";
        var tableName = "people";

        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync($"CREATE DATABASE {dbName}");

        try
        {
            await connection.ExecuteNonQueryAsync(
                $"CREATE TABLE {dbName}.{tableName} (id Int32, name String) ENGINE = Memory");

            await connection.Table<Row>($"{dbName}.{tableName}").InsertAsync(new[]
            {
                new Row { Id = 1, Name = "Alice" },
                new Row { Id = 2, Name = "Bob" },
            });

            var count = await connection.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {dbName}.{tableName}");
            Assert.Equal(2L, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP DATABASE IF EXISTS {dbName}");
        }
    }

    [Fact]
    public async Task InsertAsync_RepeatedOnSameHandle_AccumulatesRows()
    {
        // The handle returned by connection.Table<T>(name) is meant to be reused
        // across multiple writes (see the sample). Each InsertAsync opens its
        // own INSERT context — the handle itself doesn't carry per-call state.
        var tableName = $"test_repeated_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

        try
        {
            var table = connection.Table<Row>(tableName);
            for (var i = 0; i < 10; i++)
            {
                await table.InsertAsync(new Row { Id = i, Name = $"r{i}" });
            }

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(10L, count);
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
