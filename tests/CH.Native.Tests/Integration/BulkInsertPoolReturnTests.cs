using System.Reflection;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Pins that a successful bulk insert leaves its connection pool-eligible. The INSERT
/// query sets <c>_currentQueryId</c>; the bulk completion path bypasses
/// <c>ReadServerMessagesAsync</c> (whose <c>finally</c> would clear it), so unless the
/// completion clears it explicitly, <c>CanBePooled</c> stays false and any pool would
/// discard the connection — defeating pooling for bulk workloads.
/// </summary>
[Collection("ClickHouse")]
public class BulkInsertPoolReturnTests
{
    private readonly ClickHouseFixture _fixture;

    public BulkInsertPoolReturnTests(ClickHouseFixture fixture) => _fixture = fixture;

    private sealed class Row
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "name", Order = 1)] public string Name { get; set; } = "";
    }

    private static bool CanBePooled(ClickHouseConnection conn) =>
        (bool)typeof(ClickHouseConnection)
            .GetProperty("CanBePooled", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(conn)!;

    [Fact]
    public async Task CreateBulkInserterAsync_Dispose_ReturnsConnectionToPool()
    {
        var tableName = $"test_bulk_pooled_return_{Guid.NewGuid():N}";
        await using (var setup = new ClickHouseConnection(_fixture.ConnectionString))
        {
            await setup.OpenAsync();
            await setup.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");
        }

        try
        {
            await using var ds = new ClickHouseDataSource(_fixture.ConnectionString);

            await using (var inserter = await ds.CreateBulkInserterAsync<Row>(tableName))
            {
                await inserter.AddAsync(new Row { Id = 1, Name = "a" });
                await inserter.CompleteAsync();
            } // disposing the inserter must RETURN the rented connection to the pool

            var stats = ds.GetStatistics();
            // Pre-fix, BulkInserter.DisposeAsync never disposed the rented connection,
            // so it was never returned: Busy stayed 1 and Idle 0 (a connection leak).
            Assert.Equal(0, stats.Busy);
            Assert.Equal(1, stats.Idle);

            // Reuse witness: a second inserter reuses the pooled connection.
            await using (var inserter2 = await ds.CreateBulkInserterAsync<Row>(tableName))
            {
                await inserter2.AddAsync(new Row { Id = 2, Name = "b" });
                await inserter2.CompleteAsync();
            }
            Assert.Equal(1L, ds.GetStatistics().TotalCreated);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task CreateBulkInserterAsync_Dynamic_Dispose_ReturnsConnectionToPool()
    {
        var tableName = $"test_dynbulk_pooled_return_{Guid.NewGuid():N}";
        await using (var setup = new ClickHouseConnection(_fixture.ConnectionString))
        {
            await setup.OpenAsync();
            await setup.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");
        }

        try
        {
            await using var ds = new ClickHouseDataSource(_fixture.ConnectionString);

            await using (var inserter = await ds.CreateBulkInserterAsync(tableName, new[] { "id", "name" }))
            {
                await inserter.AddAsync(new object?[] { 1, "a" });
                await inserter.CompleteAsync();
            }

            var stats = ds.GetStatistics();
            Assert.Equal(0, stats.Busy);
            Assert.Equal(1, stats.Idle);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task RawBulkInserter_Dispose_DoesNotCloseCallerOwnedConnection()
    {
        // Regression guard for the ownership fix: the public constructor path must
        // NOT dispose the caller's connection — only the pooled factory transfers
        // ownership. The connection must remain open and usable after inserter dispose.
        var tableName = $"test_bulk_rawown_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");
        try
        {
            await using (var inserter = conn.CreateBulkInserter<Row>(tableName))
            {
                await inserter.AddAsync(new Row { Id = 1, Name = "a" });
                await inserter.CompleteAsync();
            }

            Assert.True(conn.IsOpen, "Caller-owned connection must stay open after raw inserter dispose.");
            Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_SuccessfulComplete_LeavesConnectionPoolable()
    {
        var tableName = $"test_bulk_poolable_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");
        try
        {
            await using (var inserter = conn.CreateBulkInserter<Row>(tableName))
            {
                await inserter.AddAsync(new Row { Id = 1, Name = "a" });
                await inserter.CompleteAsync();
            }

            // After a successful bulk insert the connection must be pool-eligible.
            // Pre-fix, _currentQueryId set by the INSERT is never cleared, so
            // CanBePooled == false and a pool would discard the connection.
            Assert.True(CanBePooled(conn),
                "Connection must be pool-eligible after a successful bulk insert.");

            // And it must actually be reusable for a subsequent query on the same wire.
            Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
            Assert.Equal(1UL, await conn.ExecuteScalarAsync<ulong>($"SELECT count() FROM {tableName}"));
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }
}
