using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Pins cancellation behavior across the three new <c>InsertAsync</c> overloads.
/// The contract: a cancelled token must surface as
/// <see cref="OperationCanceledException"/>, the underlying connection (or rented
/// pool connection) must be left clean and reusable for subsequent work, and
/// previously-flushed batches stay persisted (ClickHouse commits per data block,
/// which is documented in the BulkInserter dispose semantics).
/// </summary>
[Collection("ClickHouse")]
public class InsertAsyncCancellationTests
{
    private readonly ClickHouseFixture _fixture;

    public InsertAsyncCancellationTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task InsertAsync_TokenAlreadyCancelled_ThrowsBeforeWireActivity()
    {
        var tableName = $"test_cancel_pre_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

        try
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                connection.Table<Row>(tableName)
                    .InsertAsync(new Row { Id = 1, Name = "a" }, cancellationToken: cts.Token));

            // No row landed because we cancelled before sending.
            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(0L, count);

            // Connection is clean — a fresh insert without the cancelled token
            // must succeed, proving no orphaned protocol state.
            await connection.Table<Row>(tableName).InsertAsync(new Row { Id = 2, Name = "after" });
            count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1L, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task InsertAsync_AsyncStream_CancelledMidStream_ConnectionRemainsUsable()
    {
        var tableName = $"test_cancel_mid_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

        try
        {
            using var cts = new CancellationTokenSource();

            async IAsyncEnumerable<Row> StreamAndCancelAsync()
            {
                for (var i = 0; i < 100; i++)
                {
                    if (i == 30) cts.Cancel(); // trip the token mid-stream
                    yield return new Row { Id = i, Name = $"r{i}" };
                    await Task.Yield();
                }
            }

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                connection.Table<Row>(tableName).InsertAsync(StreamAndCancelAsync(), cancellationToken: cts.Token));

            // Whatever was flushed before the cancel may persist (per-block commit
            // semantics) — the contract is "connection still usable", not "no rows".
            // The crucial post-condition is that the next insert on this connection
            // works without protocol drift.
            await connection.Table<Row>(tableName).InsertAsync(new Row { Id = 999, Name = "after" });
            var afterCount = await connection.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName} WHERE id = 999");
            Assert.Equal(1L, afterCount);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task InsertAsync_DataSourceBound_TokenAlreadyCancelled_ConnectionReturnsToPool()
    {
        // The lease helper must release the rented connection back to the pool
        // even when the underlying insert throws OCE before sending data.
        var tableName = $"test_cancel_ds_{Guid.NewGuid():N}";
        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionString);

        await using (var setup = await dataSource.OpenConnectionAsync())
        {
            await setup.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");
        }

        try
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                dataSource.Table<Row>(tableName)
                    .InsertAsync(new Row { Id = 1, Name = "a" }, cancellationToken: cts.Token));

            Assert.Equal(0, dataSource.GetStatistics().Busy);
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
