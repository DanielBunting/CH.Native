using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Resilience;

namespace CH.Native.Samples.Queries;

/// <summary>
/// <c>ResilientConnection</c> — wraps a multi-host settings shape with retry,
/// circuit-breaker, and load-balancing policies. Models the HA setup a service
/// uses against a multi-replica ClickHouse cluster: any one replica can drop
/// out and reads continue against a healthy peer.
/// </summary>
/// <remarks>
/// Configure once via <c>ClickHouseConnectionSettingsBuilder</c>:
/// <c>WithServers(...)</c> for the replica list, <c>WithLoadBalancing(...)</c>
/// for the strategy, <c>WithResilience(...)</c> for retry / circuit breaker /
/// health check intervals. <c>ResilientConnection.QueryAsync&lt;T&gt;</c> applies
/// the policies to connection establishment; once streaming begins, transient
/// failures propagate to the caller (re-driving a partial stream is unsafe).
/// CancellationToken composes with the resilience window — cancellation wins
/// over retry.
/// </remarks>
internal static class ResilientSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_resilient_{Guid.NewGuid():N}";

        // Rebuild the settings from the user-supplied connection string and
        // layer multi-host + retry + circuit-breaker on top. In a real HA setup
        // WithServers(...) would list the actual replicas; here we just point
        // every "replica" at the same local node so the demo runs end-to-end
        // without a real cluster.
        var baseSettings = ClickHouseConnectionSettings.Parse(connectionString);
        var resilientSettings = new ClickHouseConnectionSettingsBuilder()
            .WithHost(baseSettings.Host)
            .WithPort(baseSettings.Port)
            .WithUsername(baseSettings.Username)
            .WithPassword(baseSettings.Password ?? string.Empty)
            .WithDatabase(baseSettings.Database)
            .WithServers(
                new ServerAddress(baseSettings.Host, baseSettings.Port),
                new ServerAddress(baseSettings.Host, baseSettings.Port))
            .WithLoadBalancing(LoadBalancingStrategy.RoundRobin)
            .WithResilience(r => r
                .WithRetry(new RetryOptions
                {
                    MaxRetries = 3,
                    BaseDelay = TimeSpan.FromMilliseconds(100),
                    BackoffMultiplier = 2.0,
                    MaxDelay = TimeSpan.FromSeconds(5),
                })
                .WithCircuitBreaker(new CircuitBreakerOptions())
                .WithHealthCheckInterval(TimeSpan.FromSeconds(10))
                .WithHealthCheckTimeout(TimeSpan.FromSeconds(5)))
            .Build();

        await using var resilient = new ResilientConnection(resilientSettings);
        await resilient.OpenAsync();

        try
        {
            using var cts = new CancellationTokenSource();

            await resilient.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    id    UInt32,
                    label String,
                    value Float64
                ) ENGINE = MergeTree()
                ORDER BY id
                """, cts.Token);

            await resilient.ExecuteNonQueryAsync($"""
                INSERT INTO {tableName} VALUES
                    (1, 'alpha',   12.5),
                    (2, 'beta',    24.0),
                    (3, 'gamma',   31.7),
                    (4, 'delta',   18.9),
                    (5, 'epsilon', 47.3)
                """, cts.Token);
            Console.WriteLine($"Seeded {tableName} with 5 rows via the resilient connection");

            // Scalar over the resilient surface — same shape as a plain
            // ClickHouseConnection, retry/circuit-breaker policies kick in
            // automatically on transient failures.
            var sum = await resilient.ExecuteScalarAsync<double>(
                $"SELECT sum(value) FROM {tableName}",
                cts.Token);

            // Streamed read.
            Console.WriteLine();
            Console.WriteLine("--- Resilient streamed read ---");
            await foreach (var row in resilient.QueryAsync<Datum>(
                $"SELECT id, label, value FROM {tableName} ORDER BY id",
                cts.Token))
            {
                Console.WriteLine($"  [{row.Id}] {row.Label,-8} {row.Value,6:F1}");
            }

            Console.WriteLine();
            Console.WriteLine("--- Cluster status ---");
            Console.WriteLine($"  Current server      : {resilient.CurrentServer}");
            Console.WriteLine($"  Healthy server count: {resilient.HealthyServerCount}");
            Console.WriteLine($"  Total value sum     : {sum:N1}");

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  Servers configured    : 2 (round-robin)");
            Console.WriteLine($"  Retry policy          : MaxRetries=3, BaseDelay=100ms, Backoff=2x, Max=5s");
            Console.WriteLine($"  Circuit breaker       : enabled (defaults)");
            Console.WriteLine($"  Health check interval : 10s");
            Console.WriteLine($"  Cancellation token    : threaded into every Resilient* call");
        }
        finally
        {
            await resilient.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

file class Datum
{
    [ClickHouseColumn(Name = "id")] public uint Id { get; set; }
    [ClickHouseColumn(Name = "label")] public string Label { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "value")] public double Value { get; set; }
}
