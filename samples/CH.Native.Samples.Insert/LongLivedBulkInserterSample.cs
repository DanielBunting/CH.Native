using System.Diagnostics;
using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;

namespace CH.Native.Samples.Insert;

/// <summary>
/// <c>BulkInserter&lt;T&gt;</c> — explicit Init / Add / Complete lifecycle. Models a
/// long-running ingestion process that holds the wire across many flushes,
/// amortising the INSERT handshake.
/// </summary>
/// <remarks>
/// Pick this when rows arrive over time and you want to keep one INSERT context
/// open across many batches. <c>AddAsync</c> auto-flushes when the in-memory
/// buffer reaches <c>BatchSize</c>; you can also call <c>FlushAsync</c> explicitly.
/// <c>CompleteAsync</c> sends the empty terminator block that finalises the INSERT
/// — without it the rows in the unflushed buffer are lost (DisposeAsync surfaces
/// this loudly).
/// </remarks>
internal static class LongLivedBulkInserterSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_logs_{Guid.NewGuid():N}";
        const int batchCount = 10;
        const int batchSize = 5_000;

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    ts        DateTime64(3),
                    level     LowCardinality(String),
                    service   LowCardinality(String),
                    trace_id  String,
                    message   String
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMMDD(ts)
                ORDER BY (service, ts)
                """);
            Console.WriteLine($"Created log table {tableName}");

            var rng = new Random(99);
            var levels = new[] { "TRACE", "DEBUG", "INFO", "WARN", "ERROR" };
            var services = new[] { "api", "worker", "scheduler", "sync" };
            var t0 = DateTime.UtcNow;

            var totalSw = Stopwatch.StartNew();
            await using var inserter = connection.CreateBulkInserter<LogLine>(
                tableName,
                new BulkInsertOptions { BatchSize = batchSize });
            await inserter.InitAsync();

            // Simulate batches arriving over time. AddAsync auto-flushes at BatchSize,
            // so each iteration ends with a single network flush. The wire stays
            // open across iterations — only one INSERT handshake total.
            for (var batch = 0; batch < batchCount; batch++)
            {
                var batchSw = Stopwatch.StartNew();
                for (var i = 0; i < batchSize; i++)
                {
                    var idx = batch * batchSize + i;
                    await inserter.AddAsync(new LogLine
                    {
                        Ts = t0.AddMilliseconds(idx * 7),
                        Level = levels[rng.Next(levels.Length)],
                        Service = services[rng.Next(services.Length)],
                        TraceId = Guid.NewGuid().ToString("N")[..16],
                        Message = $"event {idx} processed"
                    });
                }
                batchSw.Stop();
                Console.WriteLine($"  batch {batch + 1,2}/{batchCount}: {batchSize:N0} rows " +
                                  $"({batchSw.Elapsed.TotalMilliseconds:F0}ms)");
            }

            await inserter.CompleteAsync();
            totalSw.Stop();

            var totalRows = batchCount * batchSize;
            Console.WriteLine($"\nInserted {totalRows:N0} rows total in {totalSw.Elapsed.TotalMilliseconds:F0}ms " +
                              $"({totalRows / totalSw.Elapsed.TotalSeconds:F0} rows/sec).");

            Console.WriteLine("\n--- Log volume by level ---");
            await foreach (var row in connection.QueryAsync(
                $"SELECT level, count() AS n FROM {tableName} GROUP BY level ORDER BY n DESC"))
            {
                Console.WriteLine($"  {row["level"],-5}: {row["n"]:N0}");
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

file class LogLine
{
    [ClickHouseColumn(Name = "ts", Order = 0)] public DateTime Ts { get; set; }
    [ClickHouseColumn(Name = "level", Order = 1)] public string Level { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "service", Order = 2)] public string Service { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "trace_id", Order = 3)] public string TraceId { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "message", Order = 4)] public string Message { get; set; } = string.Empty;
}
