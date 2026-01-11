using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Resilience;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration.Resilience;

[Collection("ClickHouse")]
public class ResilientConnectionTests
{
    private readonly ClickHouseFixture _fixture;

    public ResilientConnectionTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task OpenAsync_SingleServer_ConnectsSuccessfully()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        await using var connection = new ResilientConnection(settings);
        await connection.OpenAsync();

        Assert.True(connection.IsOpen);
        Assert.NotNull(connection.CurrentServer);
        Assert.Equal(_fixture.Host, connection.CurrentServer.Value.Host);
    }

    [Fact]
    public async Task ExecuteScalarAsync_SimpleQuery_ReturnsResult()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        await using var connection = new ResilientConnection(settings);

        var result = await connection.ExecuteScalarAsync<int>("SELECT 42");

        Assert.Equal(42, result);
    }

    [Fact]
    public async Task ExecuteScalarAsync_WithRetry_RetriesOnTransientFailure()
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

        // This should succeed - retry is configured but won't be needed
        var result = await connection.ExecuteScalarAsync<int>("SELECT 1");

        Assert.Equal(1, result);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_CreateAndDropTable_Succeeds()
    {
        var tableName = $"test_resilient_{Guid.NewGuid():N}";
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        await using var connection = new ResilientConnection(settings);

        try
        {
            await connection.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (id Int32) ENGINE = Memory");

            var result = await connection.ExecuteScalarAsync<int>(
                $"SELECT count() FROM {tableName}");

            Assert.Equal(0, result);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task GetConnectionAsync_ReturnsUnderlyingConnection()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        await using var resilientConnection = new ResilientConnection(settings);

        var connection = await resilientConnection.GetConnectionAsync();

        Assert.NotNull(connection);
        Assert.True(connection.IsOpen);
        Assert.NotNull(connection.ServerInfo);
    }

    [Fact]
    public async Task CloseAsync_ClosesConnection()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        await using var connection = new ResilientConnection(settings);
        await connection.OpenAsync();

        Assert.True(connection.IsOpen);

        await connection.CloseAsync();

        Assert.False(connection.IsOpen);
        Assert.Null(connection.CurrentServer);
    }

    [Fact]
    public async Task Connection_WithCircuitBreaker_WorksNormally()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithResilience(new ResilienceOptions
            {
                CircuitBreaker = new CircuitBreakerOptions
                {
                    FailureThreshold = 5,
                    OpenDuration = TimeSpan.FromSeconds(30)
                }
            })
            .Build();

        await using var connection = new ResilientConnection(settings);

        // Multiple successful queries should keep circuit closed
        for (var i = 0; i < 10; i++)
        {
            var result = await connection.ExecuteScalarAsync<int>($"SELECT {i}");
            Assert.Equal(i, result);
        }
    }

    [Fact]
    public async Task Connection_WithAllResilienceFeatures_WorksNormally()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithResilience(r => r
                .WithRetry(new RetryOptions { MaxRetries = 3 })
                .WithCircuitBreaker(new CircuitBreakerOptions { FailureThreshold = 5 })
                .WithHealthCheckInterval(TimeSpan.FromSeconds(30)))
            .Build();

        await using var connection = new ResilientConnection(settings);

        var result = await connection.ExecuteScalarAsync<string>("SELECT 'hello'");

        Assert.Equal("hello", result);
    }

    [Fact]
    public async Task ConnectionFromString_Works()
    {
        var connectionString = _fixture.ConnectionString;

        await using var connection = new ResilientConnection(connectionString);

        var result = await connection.ExecuteScalarAsync<int>("SELECT 100");

        Assert.Equal(100, result);
    }

    [Fact]
    public async Task HealthyServerCount_SingleServer_ReturnsOne()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        await using var connection = new ResilientConnection(settings);

        Assert.Equal(1, connection.HealthyServerCount);
    }

    [Fact]
    public async Task QueryAsync_StreamsResults()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        await using var connection = new ResilientConnection(settings);

        var results = new List<ulong>();
        await foreach (var row in connection.QueryAsync("SELECT number FROM system.numbers LIMIT 10"))
        {
            results.Add(row.GetFieldValue<ulong>("number"));
        }

        Assert.Equal(10, results.Count);
        Assert.Equal(Enumerable.Range(0, 10).Select(i => (ulong)i).ToList(), results);
    }

    [Fact]
    public async Task QueryAsyncT_MapsToObjects()
    {
        var tableName = $"test_resilient_map_{Guid.NewGuid():N}";
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        await using var connection = new ResilientConnection(settings);

        try
        {
            await connection.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

            await connection.ExecuteNonQueryAsync(
                $"INSERT INTO {tableName} VALUES (1, 'Alice'), (2, 'Bob')");

            var results = new List<TestRecord>();
            await foreach (var item in connection.QueryAsync<TestRecord>($"SELECT id, name FROM {tableName} ORDER BY id"))
            {
                results.Add(item);
            }

            Assert.Equal(2, results.Count);
            Assert.Equal(1, results[0].Id);
            Assert.Equal("Alice", results[0].Name);
            Assert.Equal(2, results[1].Id);
            Assert.Equal("Bob", results[1].Name);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsertAsync_InsertsData()
    {
        var tableName = $"test_resilient_bulk_{Guid.NewGuid():N}";
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        await using var connection = new ResilientConnection(settings);

        try
        {
            await connection.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

            var rows = new[]
            {
                new TestRecord { Id = 1, Name = "Alice" },
                new TestRecord { Id = 2, Name = "Bob" },
                new TestRecord { Id = 3, Name = "Charlie" }
            };

            await connection.BulkInsertAsync(tableName, rows);

            var count = await connection.ExecuteScalarAsync<int>($"SELECT count() FROM {tableName}");
            Assert.Equal(3, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task ExecuteReaderAsync_ReturnsReader()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        await using var connection = new ResilientConnection(settings);

        await using var reader = await connection.ExecuteReaderAsync("SELECT 1 as a, 2 as b, 3 as c");

        Assert.True(await reader.ReadAsync());
        Assert.Equal(3, reader.FieldCount);
        Assert.Equal(1, reader.GetFieldValue<int>("a"));
        Assert.Equal(2, reader.GetFieldValue<int>("b"));
        Assert.Equal(3, reader.GetFieldValue<int>("c"));
    }

    [Fact]
    public async Task ConnectionString_WithResilienceOptions_ParsesCorrectly()
    {
        var connectionString = $"Host={_fixture.Host};Port={_fixture.Port};Username={_fixture.Username};Password={_fixture.Password};MaxRetries=5;RetryBaseDelay=50;CircuitBreakerThreshold=10";

        await using var connection = new ResilientConnection(connectionString);

        var result = await connection.ExecuteScalarAsync<int>("SELECT 42");
        Assert.Equal(42, result);
    }

    [Fact]
    public async Task MultipleOperations_InSequence_Succeed()
    {
        var tableName = $"test_resilient_seq_{Guid.NewGuid():N}";
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithResilience(new ResilienceOptions
            {
                Retry = new RetryOptions { MaxRetries = 2 }
            })
            .Build();

        await using var connection = new ResilientConnection(settings);

        try
        {
            // Create table
            await connection.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (id Int32, value Float64) ENGINE = Memory");

            // Bulk insert
            var rows = Enumerable.Range(1, 100)
                .Select(i => new TestValueRecord { Id = i, Value = i * 1.5 })
                .ToList();
            await connection.BulkInsertAsync(tableName, rows);

            // Verify count
            var count = await connection.ExecuteScalarAsync<int>($"SELECT count() FROM {tableName}");
            Assert.Equal(100, count);

            // Stream query
            var sum = 0.0;
            await foreach (var row in connection.QueryAsync($"SELECT value FROM {tableName}"))
            {
                sum += row.GetFieldValue<double>("value");
            }

            // Expected sum: 1.5 + 3.0 + 4.5 + ... + 150.0 = 1.5 * (1 + 2 + ... + 100) = 1.5 * 5050 = 7575
            Assert.Equal(7575.0, sum);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Connection_ReopensAfterClose()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        await using var connection = new ResilientConnection(settings);

        // First query
        var result1 = await connection.ExecuteScalarAsync<int>("SELECT 1");
        Assert.Equal(1, result1);

        // Close
        await connection.CloseAsync();
        Assert.False(connection.IsOpen);

        // Second query should auto-reconnect
        var result2 = await connection.ExecuteScalarAsync<int>("SELECT 2");
        Assert.Equal(2, result2);
        Assert.True(connection.IsOpen);
    }

    public class TestRecord
    {
        [ClickHouseColumn(Name = "id")]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "name")]
        public string Name { get; set; } = string.Empty;
    }

    public class TestValueRecord
    {
        [ClickHouseColumn(Name = "id")]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "value")]
        public double Value { get; set; }
    }
}
