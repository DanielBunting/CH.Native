using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class CancellationTests
{
    private readonly ClickHouseFixture _fixture;

    public CancellationTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ExecuteScalarAsync_WithTimeout_ThrowsOperationCancelled()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));

        // Use a large numbers() query that takes time
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await connection.ExecuteScalarAsync<long>(
                "SELECT count() FROM numbers(10000000000)",
                cancellationToken: cts.Token);
        });
    }

    [Fact]
    public async Task ExecuteScalarAsync_AfterCancellation_ConnectionStillUsable()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Cancel a long-running query
        using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500)))
        {
            try
            {
                await connection.ExecuteScalarAsync<long>(
                    "SELECT count() FROM numbers(10000000000)",
                    cancellationToken: cts.Token);
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }

        // Give server time to process the cancellation
        await Task.Delay(200);

        // Connection should still be usable for new queries
        var result = await connection.ExecuteScalarAsync<int>("SELECT 42");
        Assert.Equal(42, result);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_WithTimeout_ThrowsOperationCancelled()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await connection.ExecuteNonQueryAsync(
                "SELECT count() FROM numbers(10000000000)",
                cancellationToken: cts.Token);
        });
    }

    [Fact]
    public async Task QueryAsync_EarlyBreak_ConnectionStillUsable()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Start streaming a large result set but break early
        int count = 0;
        await foreach (var row in connection.QueryAsync("SELECT number FROM numbers(1000000)"))
        {
            count++;
            if (count >= 10)
                break; // Early break should trigger cancellation
        }

        // Give server time to process the cancellation
        await Task.Delay(100);

        // Connection should still be usable
        var result = await connection.ExecuteScalarAsync<int>("SELECT 123");
        Assert.Equal(123, result);
    }

    [Fact]
    public async Task ExecuteReaderAsync_EarlyDispose_ConnectionStillUsable()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Start reading but dispose early
        await using (var reader = await connection.ExecuteReaderAsync("SELECT number FROM numbers(1000000)"))
        {
            // Read a few rows
            for (int i = 0; i < 5; i++)
            {
                await reader.ReadAsync();
            }
            // Reader disposed without reading all rows - should trigger cancel
        }

        // Give server time to process
        await Task.Delay(100);

        // Connection should still be usable
        var result = await connection.ExecuteScalarAsync<int>("SELECT 456");
        Assert.Equal(456, result);
    }

    [Fact]
    public async Task CancelCurrentQueryAsync_CancelsRunningQuery()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Start a long-running query in a background task
        var queryTask = Task.Run(async () =>
        {
            await connection.ExecuteScalarAsync<long>("SELECT count() FROM numbers(10000000000)");
        });

        // Give the query time to start
        await Task.Delay(200);

        // Cancel it
        await connection.CancelCurrentQueryAsync();

        // The query task should complete (either with exception or result)
        // The important thing is it doesn't take forever
        var completedInTime = await Task.WhenAny(queryTask, Task.Delay(10000)) == queryTask;
        Assert.True(completedInTime, "Query should have completed after cancellation");
    }

    [Fact]
    public async Task CancelCurrentQueryAsync_WhenNoQuery_DoesNothing()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Should not throw when no query is running
        await connection.CancelCurrentQueryAsync();

        // Connection should still work
        var result = await connection.ExecuteScalarAsync<int>("SELECT 789");
        Assert.Equal(789, result);
    }

    [Fact]
    public async Task QueryAsync_WithCancellationToken_CancelsIteration()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(800));
        int count = 0;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            // Large result set - cancellation should kick in while streaming
            await foreach (var row in connection.QueryAsync("SELECT number FROM numbers(10000000000)", cts.Token))
            {
                count++;
            }
        });

        // Should have read some rows before cancellation
        Assert.True(count > 0, $"Expected some rows before cancellation, got {count}");
    }

    [Fact]
    public async Task CurrentQueryId_DuringQuery_ReturnsQueryId()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Before query, should be null
        Assert.Null(connection.CurrentQueryId);

        string? capturedQueryId = null;

        // Start a query and capture the ID mid-execution
        var queryTask = Task.Run(async () =>
        {
            await connection.ExecuteScalarAsync<long>("SELECT count() FROM numbers(10000000000)");
        });

        // Wait a bit for query to start
        await Task.Delay(100);

        // Capture query ID while running
        capturedQueryId = connection.CurrentQueryId;

        // Cancel to avoid waiting forever
        await connection.CancelCurrentQueryAsync();

        try { await queryTask; } catch { /* expected */ }

        // Should have captured a valid GUID
        Assert.NotNull(capturedQueryId);
        Assert.True(Guid.TryParse(capturedQueryId, out _), "Query ID should be a valid GUID");
    }

    [Fact]
    public async Task KillQueryAsync_KillsRunningQuery()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        string? queryId = null;

        // Start a long-running query
        var queryTask = Task.Run(async () =>
        {
            await connection.ExecuteScalarAsync<long>("SELECT count() FROM numbers(10000000000)");
        });

        // Wait for query to start and capture its ID
        await Task.Delay(200);
        queryId = connection.CurrentQueryId;
        Assert.NotNull(queryId);

        // Kill the query using a separate connection
        await using var killerConnection = new ClickHouseConnection(_fixture.ConnectionString);
        await killerConnection.OpenAsync();
        await killerConnection.KillQueryAsync(queryId!);

        // The original query should complete (with error or success)
        var completedInTime = await Task.WhenAny(queryTask, Task.Delay(10000)) == queryTask;
        Assert.True(completedInTime, "Query should have been killed");
    }

    [Fact]
    public async Task KillQueryAsync_InvalidQueryId_ThrowsArgumentException()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await connection.KillQueryAsync("not-a-valid-guid");
        });
    }

    [Fact]
    public async Task KillQueryAsync_NullQueryId_ThrowsArgumentNullException()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await connection.KillQueryAsync(null!);
        });
    }

    [Fact]
    public async Task KillQueryAsync_NonExistentQueryId_Succeeds()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // KILL QUERY with non-existent ID should succeed (no-op)
        var randomQueryId = Guid.NewGuid().ToString("D");
        await connection.KillQueryAsync(randomQueryId);

        // Connection should still work
        var result = await connection.ExecuteScalarAsync<int>("SELECT 1");
        Assert.Equal(1, result);
    }
}
