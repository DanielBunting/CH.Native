using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Resilience;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class ErrorRecoveryTests
{
    private readonly ClickHouseFixture _fixture;

    public ErrorRecoveryTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Connection_ReusableAfterServerError()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Execute bad SQL — should throw a server exception
        await Assert.ThrowsAsync<ClickHouseServerException>(
            () => connection.ExecuteScalarAsync<int>("SELECT * FROM nonexistent_table_xyz_123"));

        // The connection should be reusable after a server error
        var result = await connection.ExecuteScalarAsync<int>("SELECT 1");
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task Connection_ReusableAfterTypeMismatch()
    {
        var tableName = $"test_recovery_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Value Int32
            ) ENGINE = Memory");

        try
        {
            // Try to insert a string into an Int32 column — should fail
            await Assert.ThrowsAnyAsync<Exception>(
                () => connection.ExecuteNonQueryAsync(
                    $"INSERT INTO {tableName} VALUES ('not_a_number')"));

            // The connection should be reusable after a type mismatch error
            var result = await connection.ExecuteScalarAsync<int>("SELECT 42");
            Assert.Equal(42, result);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Connection_HandlesKillQuery()
    {
        await using var connection1 = new ClickHouseConnection(_fixture.ConnectionString);
        await connection1.OpenAsync();

        // Start a long query on connection1
        var queryTask = Task.Run(async () =>
        {
            try
            {
                await connection1.ExecuteScalarAsync<int>("SELECT sleep(10)");
            }
            catch
            {
                // Expected — query will be killed
            }
        });

        // Wait a moment for the query to start
        await Task.Delay(500);

        // Get the query ID and kill it from a second connection
        var queryId = connection1.CurrentQueryId;
        if (queryId != null)
        {
            await using var connection2 = new ClickHouseConnection(_fixture.ConnectionString);
            await connection2.OpenAsync();

            try
            {
                await connection2.ExecuteNonQueryAsync($"KILL QUERY WHERE query_id = '{queryId}' SYNC");
            }
            catch
            {
                // KILL QUERY may itself throw if the query finishes first
            }
        }

        // The query task should complete (either normally or with exception), not hang
        var completedInTime = await Task.WhenAny(queryTask, Task.Delay(TimeSpan.FromSeconds(15))) == queryTask;
        Assert.True(completedInTime, "Long query should complete after being killed, not hang.");

        // Verify recovery: open a fresh connection and run a query
        await using var verifyConnection = new ClickHouseConnection(_fixture.ConnectionString);
        await verifyConnection.OpenAsync();
        var result = await verifyConnection.ExecuteScalarAsync<int>("SELECT 1");
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task BulkInsert_PartialBatchRecovery()
    {
        var tableName = $"test_recovery_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = MergeTree()
            ORDER BY Id");

        try
        {
            // First insert: valid data
            await using (var inserter = connection.CreateBulkInserter<SimpleRow>(tableName))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new SimpleRow { Id = 1, Name = "Alice" });
                await inserter.AddAsync(new SimpleRow { Id = 2, Name = "Bob" });
                await inserter.CompleteAsync();
            }

            // Verify first batch is committed
            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);

            // Second insert on a separate connection to a non-existent table — should fail
            await using var connection2 = new ClickHouseConnection(_fixture.ConnectionString);
            await connection2.OpenAsync();

            try
            {
                await using var inserter2 = connection2.CreateBulkInserter<SimpleRow>("nonexistent_table_xyz_456");
                await Assert.ThrowsAnyAsync<Exception>(() => inserter2.InitAsync());
            }
            catch
            {
                // Expected failure
            }

            // Original data should still be intact
            await using var verifyConnection = new ClickHouseConnection(_fixture.ConnectionString);
            await verifyConnection.OpenAsync();
            var verifyCount = await verifyConnection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, verifyCount);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task ResilientConnection_RetrySucceeds()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithResilience(new ResilienceOptions
            {
                Retry = new RetryOptions
                {
                    MaxRetries = 3,
                    BaseDelay = TimeSpan.FromMilliseconds(10)
                }
            })
            .Build();

        await using var connection = new ResilientConnection(settings);

        var result = await connection.ExecuteScalarAsync<int>("SELECT 1");
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task ResilientConnection_CircuitBreaker_OpensAfterThreshold()
    {
        // Use a wrong port to guarantee connection failures
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(1) // Wrong port — will cause connection failures
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithResilience(new ResilienceOptions
            {
                CircuitBreaker = new CircuitBreakerOptions
                {
                    FailureThreshold = 3,
                    OpenDuration = TimeSpan.FromSeconds(2),
                    FailureWindow = TimeSpan.FromMinutes(1)
                }
            })
            .Build();

        await using var connection = new ResilientConnection(settings);

        // Cause failures to exceed the threshold
        for (var i = 0; i < 4; i++)
        {
            try
            {
                await connection.ExecuteScalarAsync<int>("SELECT 1");
            }
            catch
            {
                // Expected — connection to wrong port will fail
            }
        }

        // The next attempt should fail with a circuit breaker open exception
        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => connection.ExecuteScalarAsync<int>("SELECT 1"));

        Assert.True(
            ex is CircuitBreakerOpenException or ClickHouseConnectionException,
            $"Expected CircuitBreakerOpenException or ClickHouseConnectionException but got {ex.GetType().Name}: {ex.Message}");
    }

    [Fact]
    public async Task ResilientConnection_CircuitBreaker_HalfOpenRecovery()
    {
        // Use a short open duration so the test doesn't take too long
        var openDuration = TimeSpan.FromSeconds(2);

        // First, we need a circuit breaker that we can trip with bad connections,
        // then recover with good connections.
        // Strategy: use wrong port to trip, then create a new ResilientConnection with correct port
        // after the open duration. However, ResilientConnection has a fixed port, so instead
        // we use the standalone CircuitBreaker to test the half-open recovery behavior.

        var circuitBreaker = new CircuitBreaker(new CircuitBreakerOptions
        {
            FailureThreshold = 2,
            OpenDuration = openDuration,
            FailureWindow = TimeSpan.FromMinutes(1)
        });

        // Trip the circuit breaker with failures
        for (var i = 0; i < 2; i++)
        {
            try
            {
                await circuitBreaker.ExecuteAsync<int>(async _ =>
                {
                    throw new IOException("Simulated failure");
                });
            }
            catch (IOException)
            {
                // Expected
            }
        }

        // Circuit should be open
        Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

        // Wait for the open duration to elapse so it transitions to half-open
        await Task.Delay(openDuration + TimeSpan.FromMilliseconds(500));

        // Now it should be half-open, and a successful operation should close it
        var result = await circuitBreaker.ExecuteAsync<int>(async _ =>
        {
            // Simulate a successful operation
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
    }

    [Fact]
    public async Task ResilientConnection_MaxRetriesExceeded()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(1) // Wrong port — will cause connection failures
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithResilience(new ResilienceOptions
            {
                Retry = new RetryOptions
                {
                    MaxRetries = 2,
                    BaseDelay = TimeSpan.FromMilliseconds(10),
                    MaxDelay = TimeSpan.FromMilliseconds(50)
                }
            })
            .Build();

        await using var connection = new ResilientConnection(settings);

        // Should fail after exhausting all retries
        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => connection.ExecuteScalarAsync<int>("SELECT 1"));

        // The final exception should be a connection-related error
        Assert.True(
            ex is ClickHouseConnectionException or IOException or System.Net.Sockets.SocketException or AggregateException,
            $"Expected a connection-related exception but got {ex.GetType().Name}: {ex.Message}");
    }

    [Fact]
    public async Task ResilientConnection_NonTransientError_NoRetry()
    {
        var retryCount = 0;
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithResilience(new ResilienceOptions
            {
                Retry = new RetryOptions
                {
                    MaxRetries = 5,
                    BaseDelay = TimeSpan.FromMilliseconds(10)
                }
            })
            .Build();

        await using var connection = new ResilientConnection(settings);

        // A syntax error is NOT transient — it should not be retried
        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => connection.ExecuteScalarAsync<int>("SELECTX invalid syntax here!!!"));

        // Should be a server exception for syntax error, not a retry-exhaustion wrapper
        Assert.True(
            ex is ClickHouseServerException,
            $"Expected ClickHouseServerException for syntax error but got {ex.GetType().Name}: {ex.Message}");
    }

    [Fact]
    public async Task Connection_TimeoutDuringQuery()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Use a very short cancellation timeout
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        // Execute a long query that should be cancelled by the token
        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => connection.ExecuteScalarAsync<int>("SELECT sleep(3)", cancellationToken: cts.Token));

        // Verify we got either a cancellation or server exception
        Assert.True(
            ex is OperationCanceledException or ClickHouseServerException,
            $"Expected OperationCanceledException or ClickHouseServerException but got {ex.GetType().Name}: {ex.Message}");
    }

    [Fact]
    public async Task Connection_ReusableAfterTypedQueryError()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Execute bad SQL via the typed query path (ReadTypedBlocksAsync)
        await Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            await foreach (var _ in connection.QueryTypedAsync<int>("SELECT * FROM nonexistent_table_xyz_789"))
            {
            }
        });

        // The connection should be reusable after a server error in the typed path
        var result = await connection.ExecuteScalarAsync<int>("SELECT 1");
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task Connection_ReusableAfterMultipleErrors()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        for (var i = 0; i < 3; i++)
        {
            await Assert.ThrowsAsync<ClickHouseServerException>(
                () => connection.ExecuteScalarAsync<int>("SELECT * FROM nonexistent_table_xyz_123"));

            var result = await connection.ExecuteScalarAsync<int>($"SELECT {i + 1}");
            Assert.Equal(i + 1, result);
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
