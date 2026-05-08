using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;

namespace CH.Native.Samples.Queries;

/// <summary>
/// <c>ClickHouseDataSource</c> — pooled query reads. Models a service that fans
/// out concurrent reads from many request handlers, each renting a connection
/// from a shared pool for the lifetime of the call. Demonstrates both the
/// <c>dataSource.Table&lt;T&gt;()</c> LINQ entry point and the rented-connection
/// raw-SQL path.
/// </summary>
/// <remarks>
/// The data source is long-lived (a singleton in a typical app); each
/// <c>dataSource.Table&lt;T&gt;()</c> enumeration and each
/// <c>dataSource.OpenConnectionAsync()</c> rents a connection on entry and
/// returns it on completion. Concurrent reads from multiple threads are safe —
/// each rents its own physical connection up to <c>MaxPoolSize</c>, then queues.
/// </remarks>
internal static class DataSourcePooledSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_pool_metrics_{Guid.NewGuid():N}";
        const int workers = 8;
        const int rowsPerSensor = 5_000;

        await using var dataSource = new ClickHouseDataSource(connectionString);
        Console.WriteLine($"Initialised pool, target table {tableName}");

        try
        {
            // Setup + seed via a rented connection.
            await using (var setup = await dataSource.OpenConnectionAsync())
            {
                await setup.ExecuteNonQueryAsync($"""
                    CREATE TABLE {tableName} (
                        sensor_id   UInt32,
                        observed_at DateTime64(3),
                        metric      LowCardinality(String),
                        value       Float64
                    ) ENGINE = MergeTree()
                    ORDER BY (sensor_id, observed_at)
                    """);

                // sensor_id ranges 0..workers-1 so each worker's per-sensor
                // sum below covers a distinct slice and the sum-of-sums equals
                // the global sum.
                await setup.ExecuteNonQueryAsync($"""
                    INSERT INTO {tableName}
                    SELECT
                        number % {workers} AS sensor_id,
                        toDateTime64('2026-05-01 00:00:00', 3) + number / 1000 AS observed_at,
                        ['cpu','mem','disk','net'][number % 4 + 1] AS metric,
                        round(20 + 80 * (intHash32(number) / 4294967295.0), 2) AS value
                    FROM numbers({workers * rowsPerSensor})
                    """);
            }
            Console.WriteLine($"Seeded {tableName} with {workers * rowsPerSensor:N0} rows");

            using var cts = new CancellationTokenSource();
            var beforeStats = dataSource.GetStatistics();
            var sw = Stopwatch.StartNew();

            // Concurrent fan-out: each worker queries one sensor's recent
            // average via the LINQ entry point on the data source. Each
            // enumeration rents and returns its own connection.
            var sumByWorker = new double[workers];
            var tasks = Enumerable.Range(0, workers).Select(async sensorId =>
            {
                var totalForSensor = await dataSource.Table<Sample>(tableName)
                    .Where(s => s.SensorId == (uint)sensorId)
                    .SumAsync(s => s.Value, cts.Token);
                sumByWorker[sensorId] = totalForSensor;
            });
            await Task.WhenAll(tasks);
            sw.Stop();

            var afterStats = dataSource.GetStatistics();

            // One more rented-connection query for a global rollup, threading
            // a custom queryId so users can correlate it server-side.
            var globalQueryId = $"pool-global-rollup-{Guid.NewGuid():N}";
            await using var verify = await dataSource.OpenConnectionAsync(cts.Token);
            var globalSum = await verify.ExecuteScalarAsync<double>(
                $"SELECT sum(value) FROM {tableName}",
                cancellationToken: cts.Token,
                queryId: globalQueryId);

            Console.WriteLine();
            Console.WriteLine("--- Per-sensor sums (8 concurrent workers) ---");
            for (var i = 0; i < workers; i++)
            {
                Console.WriteLine($"  sensor #{i}  sum={sumByWorker[i]:N1}");
            }
            Console.WriteLine($"\nFan-out completed in {sw.Elapsed.TotalMilliseconds:F0}ms.");
            Console.WriteLine($"Global sum (rented connection): {globalSum:N1}");
            Console.WriteLine($"Sum-of-sums                   : {sumByWorker.Sum():N1}");

            Console.WriteLine();
            Console.WriteLine("--- Pool stats ---");
            Console.WriteLine($"  before: total={beforeStats.Total}, idle={beforeStats.Idle}, busy={beforeStats.Busy}, served={beforeStats.TotalRentsServed}");
            Console.WriteLine($"  after : total={afterStats.Total}, idle={afterStats.Idle}, busy={afterStats.Busy}, served={afterStats.TotalRentsServed}");

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  Cancellation token : threaded into SumAsync, OpenConnectionAsync, ExecuteScalarAsync");
            Console.WriteLine($"  queryId sent       : {globalQueryId}");
            Console.WriteLine($"  queryId echoed     : {verify.LastQueryId}");
        }
        finally
        {
            await using var teardown = await dataSource.OpenConnectionAsync();
            await teardown.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

file class Sample
{
    [ClickHouseColumn(Name = "sensor_id")] public uint SensorId { get; set; }
    [ClickHouseColumn(Name = "observed_at")] public DateTime ObservedAt { get; set; }
    [ClickHouseColumn(Name = "metric")] public string Metric { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "value")] public double Value { get; set; }
}
