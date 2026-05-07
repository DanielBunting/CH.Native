using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;

namespace CH.Native.Samples.Insert;

/// <summary>
/// <c>dataSource.Table&lt;T&gt;(name).InsertAsync(rows)</c> — pooled rent-per-call.
/// Models a service that fans out concurrent ingestion writes from multiple
/// request handlers, each taking a connection from a shared pool.
/// </summary>
/// <remarks>
/// The data source is long-lived (a singleton in a typical app); the table handle
/// returned by <c>dataSource.Table&lt;T&gt;()</c> rents a connection from the
/// pool for the lifetime of each <c>InsertAsync</c> (or each LINQ enumeration),
/// then returns it. Concurrent inserts from multiple threads are safe — each
/// rents its own connection — so the same handle can be shared across the
/// app without locking.
/// </remarks>
internal static class DataSourcePooledSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_metrics_{Guid.NewGuid():N}";
        const int workers = 8;
        const int rowsPerWorker = 5_000;

        await using var dataSource = new ClickHouseDataSource(connectionString);
        Console.WriteLine($"Initialised pool, target table {tableName}");

        try
        {
            await using (var setupConn = await dataSource.OpenConnectionAsync())
            {
                await setupConn.ExecuteNonQueryAsync($"""
                    CREATE TABLE {tableName} (
                        observed_at DateTime64(3),
                        worker_id   UInt32,
                        metric_name LowCardinality(String),
                        value       Float64
                    ) ENGINE = MergeTree()
                    ORDER BY (metric_name, observed_at)
                    """);
            }

            // Concurrent fan-out: each worker rents its own connection, runs an
            // independent BulkInsert, returns the connection on completion. The
            // pool's MaxPoolSize gates how many physical connections actually
            // open simultaneously — ramped beyond that and the rest queue.
            var sw = Stopwatch.StartNew();
            var tasks = Enumerable.Range(0, workers).Select(async workerId =>
            {
                var rng = new Random(workerId);
                var t0 = DateTime.UtcNow;
                var rows = Enumerable.Range(0, rowsPerWorker).Select(i => new MetricSample
                {
                    ObservedAt = t0.AddMilliseconds(i),
                    WorkerId = (uint)workerId,
                    MetricName = (i % 3) switch { 0 => "cpu", 1 => "mem", _ => "disk" },
                    Value = Math.Round(rng.NextDouble() * 100.0, 2)
                });
                await dataSource.Table<MetricSample>(tableName).InsertAsync(rows);
            });
            await Task.WhenAll(tasks);
            sw.Stop();

            var totalRows = workers * rowsPerWorker;
            Console.WriteLine($"Inserted {totalRows:N0} rows across {workers} concurrent workers " +
                              $"in {sw.Elapsed.TotalMilliseconds:F0}ms.");

            var stats = dataSource.GetStatistics();
            Console.WriteLine($"Pool: total={stats.Total}, idle={stats.Idle}, busy={stats.Busy}, " +
                              $"served={stats.TotalRentsServed}");

            await using var verifyConn = await dataSource.OpenConnectionAsync();
            Console.WriteLine("\n--- Mean value per metric ---");
            await foreach (var row in verifyConn.QueryAsync(
                $"""
                SELECT metric_name, round(avg(value), 2) AS mean
                FROM {tableName}
                GROUP BY metric_name
                ORDER BY metric_name
                """))
            {
                Console.WriteLine($"  {row["metric_name"]}: {row["mean"]}");
            }
        }
        finally
        {
            await using var teardownConn = await dataSource.OpenConnectionAsync();
            await teardownConn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

file class MetricSample
{
    [ClickHouseColumn(Name = "observed_at", Order = 0)] public DateTime ObservedAt { get; set; }
    [ClickHouseColumn(Name = "worker_id", Order = 1)] public uint WorkerId { get; set; }
    [ClickHouseColumn(Name = "metric_name", Order = 2)] public string MetricName { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "value", Order = 3)] public double Value { get; set; }
}
