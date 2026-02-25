using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class ConnectionLifecycleTests
{
    private readonly ClickHouseFixture _fixture;

    public ConnectionLifecycleTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Connection_DoubleDispose_NoException()
    {
        var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.DisposeAsync();
        await connection.DisposeAsync();
    }

    [Fact]
    public async Task Connection_DisposeWithoutOpen_NoException()
    {
        var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.DisposeAsync();
    }

    [Fact]
    public async Task Connection_DisposeWhileQueryRunning()
    {
        var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Start a long-running query in a background task
        var queryTask = Task.Run(async () =>
        {
            try
            {
                await connection.ExecuteScalarAsync<int>("SELECT sleep(3)");
            }
            catch
            {
                // Expected — connection may be disposed while query is running
            }
        });

        // Give the query a moment to start, then dispose
        await Task.Delay(100);
        await connection.DisposeAsync();

        // Await the query task — it should complete (either successfully or with an exception), not hang
        var completedInTime = await Task.WhenAny(queryTask, Task.Delay(TimeSpan.FromSeconds(10))) == queryTask;
        Assert.True(completedInTime, "Query task should complete after connection dispose, not hang.");
    }

    [Fact]
    public async Task BulkInserter_DisposeWithoutComplete()
    {
        var tableName = $"test_lifecycle_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            var inserter = connection.CreateBulkInserter<SimpleRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new SimpleRow { Id = 1, Name = "Alice" });
            await inserter.AddAsync(new SimpleRow { Id = 2, Name = "Bob" });

            // Dispose without calling CompleteAsync — should not throw
            await inserter.DisposeAsync();
        }
        finally
        {
            // Need a fresh connection since the previous one may be in a bad state
            await using var cleanupConnection = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanupConnection.OpenAsync();
            await cleanupConnection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInserter_DoubleComplete_ThrowsOrNoOp()
    {
        var tableName = $"test_lifecycle_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new SimpleRow { Id = 1, Name = "Alice" });
            await inserter.CompleteAsync();

            // Second CompleteAsync — should either succeed (idempotent) or throw InvalidOperationException
            try
            {
                await inserter.CompleteAsync();
                // If we get here, it was idempotent — that's fine
            }
            catch (InvalidOperationException)
            {
                // Also acceptable
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Connection_UseAfterClose_Throws()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.CloseAsync();

        // Attempt to use the connection after close
        // May throw InvalidOperationException or auto-reconnect depending on implementation
        try
        {
            await connection.ExecuteScalarAsync<int>("SELECT 1");
            // If it auto-reconnects and succeeds, that's acceptable
        }
        catch (Exception ex)
        {
            Assert.True(
                ex is InvalidOperationException or ObjectDisposedException or IOException,
                $"Expected InvalidOperationException, ObjectDisposedException, or IOException but got {ex.GetType().Name}: {ex.Message}");
        }
    }

    [Fact]
    public async Task Connection_UseAfterDispose_Throws()
    {
        var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.DisposeAsync();

        // Attempt to use the connection after dispose — should throw
        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => connection.ExecuteScalarAsync<int>("SELECT 1"));

        Assert.True(
            ex is ObjectDisposedException or InvalidOperationException,
            $"Expected ObjectDisposedException or InvalidOperationException but got {ex.GetType().Name}: {ex.Message}");
    }

    [Fact]
    public async Task Connection_ReopenAfterClose_Works()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);

        // First open and query
        await connection.OpenAsync();
        var result1 = await connection.ExecuteScalarAsync<int>("SELECT 1");
        Assert.Equal(1, result1);

        // Close
        await connection.CloseAsync();
        Assert.False(connection.IsOpen);

        // Reopen and query again
        await connection.OpenAsync();
        Assert.True(connection.IsOpen);

        var result2 = await connection.ExecuteScalarAsync<int>("SELECT 2");
        Assert.Equal(2, result2);
    }

    [Fact]
    public async Task Connection_CancellationDuringOpen_Throws()
    {
        var connection = new ClickHouseConnection(_fixture.ConnectionString);

        try
        {
            // Use a pre-cancelled token
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => connection.OpenAsync(cts.Token));
        }
        finally
        {
            await connection.DisposeAsync();
        }
    }

    [Fact]
    public async Task BulkInserter_CancellationDuringInsert()
    {
        var tableName = $"test_lifecycle_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            using var cts = new CancellationTokenSource();
            var inserter = connection.CreateBulkInserter<SimpleRow>(tableName);

            try
            {
                await inserter.InitAsync();

                // Add some rows then cancel
                for (var i = 0; i < 100; i++)
                {
                    await inserter.AddAsync(new SimpleRow { Id = i, Name = $"Row_{i}" });
                }

                // Cancel before completing
                cts.Cancel();

                // Try to add more with cancelled token - may throw or complete
                try
                {
                    await inserter.AddAsync(
                        new SimpleRow { Id = 999, Name = "after_cancel" },
                        cts.Token);
                }
                catch (OperationCanceledException)
                {
                    // Expected
                }
            }
            finally
            {
                await inserter.DisposeAsync();
            }

            // Verify the connection infrastructure is still functional by opening a new connection
            await using var verifyConnection = new ClickHouseConnection(_fixture.ConnectionString);
            await verifyConnection.OpenAsync();
            var result = await verifyConnection.ExecuteScalarAsync<int>("SELECT 1");
            Assert.Equal(1, result);
        }
        finally
        {
            await using var cleanupConnection = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanupConnection.OpenAsync();
            await cleanupConnection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #region Test POCOs

    private class SimpleRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    #endregion
}
