using System.Diagnostics;
using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;

namespace CH.Native.Samples.Insert;

/// <summary>
/// <c>dataSource.BulkInsertAsync(...)</c> and <c>ParallelBulkInserter&lt;T&gt;</c> —
/// fan a single large insert out across multiple pooled connections ("pipes").
/// This is the built-in form of the manual fan-out in
/// <see cref="DataSourcePooledSample"/>: one logical insert, N workers, with
/// backpressure, error aggregation, and a committed-row count — useful when one
/// stream can't saturate throughput (you're CPU-bound serializing rows, or you
/// want ClickHouse to build MergeTree parts on parallel server threads).
/// </summary>
/// <remarks>
/// A parallel insert is <b>not atomic</b> and supports <b>no deduplication token</b> —
/// rows commit out of order across workers and a mid-stream failure leaves
/// already-flushed blocks persisted. Handle retry idempotency above the inserter
/// (staging-table swap or an idempotent engine). Size <c>DegreeOfParallelism</c>
/// comfortably below the pool's <c>MaxPoolSize</c>; the best value is
/// hardware-dependent, so benchmark it on representative hardware.
/// </remarks>
internal static class ParallelBulkInsertSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_readings_{Guid.NewGuid():N}";
        const int rowCount = 500_000;

        await using var dataSource = new ClickHouseDataSource(connectionString);
        Console.WriteLine($"Initialised pool, target table {tableName}");

        try
        {
            await using (var setup = await dataSource.OpenConnectionAsync())
            {
                await setup.ExecuteNonQueryAsync($"""
                    CREATE TABLE {tableName} (
                        id        Int64,
                        sensor    LowCardinality(String),
                        reading   Float64,
                        recorded  DateTime64(3)
                    ) ENGINE = MergeTree()
                    ORDER BY id
                    """);
            }

            var options = new ParallelBulkInsertOptions { DegreeOfParallelism = 4, BatchSize = 50_000 };

            // --- One-shot helper: consume a source, return the committed row count ---
            var sw = Stopwatch.StartNew();
            long written = await dataSource.BulkInsertAsync(tableName, GenerateRows(rowCount), options);
            sw.Stop();
            Console.WriteLine($"One-shot:  {written:N0} rows across {options.DegreeOfParallelism} pipes " +
                              $"in {sw.Elapsed.TotalMilliseconds:F0}ms.");

            // --- Streaming inserter: push rows yourself, then observe RowsWritten ---
            sw.Restart();
            await using (var inserter = await dataSource.CreateParallelBulkInserterAsync<SensorReading>(tableName, options))
            {
                foreach (var row in GenerateRows(rowCount))
                    await inserter.AddAsync(row);             // awaits when the channel is full (backpressure)
                await inserter.CompleteAsync();               // flush + commit + surface any worker failure
                Console.WriteLine($"Streaming: {inserter.RowsWritten:N0} rows across {inserter.DegreeOfParallelism} pipes " +
                                  $"in {sw.Elapsed.TotalMilliseconds:F0}ms.");
            }

            await using var verify = await dataSource.OpenConnectionAsync();
            var total = await verify.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Console.WriteLine($"\nTable now holds {total:N0} rows (both passes, 2 × {rowCount:N0}).");

            var stats = dataSource.GetStatistics();
            Console.WriteLine($"Pool: total={stats.Total}, idle={stats.Idle}, busy={stats.Busy}, served={stats.TotalRentsServed}");
        }
        finally
        {
            await using var teardown = await dataSource.OpenConnectionAsync();
            await teardown.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }

        // Local function so the file-scoped row type stays out of any type-member
        // signature (file types can't appear there).
        static IEnumerable<SensorReading> GenerateRows(int count)
        {
            var rng = new Random(42);
            var t0 = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            for (int i = 0; i < count; i++)
            {
                yield return new SensorReading
                {
                    Id = i,
                    Sensor = (i % 4) switch { 0 => "north", 1 => "south", 2 => "east", _ => "west" },
                    Reading = Math.Round(rng.NextDouble() * 100.0, 3),
                    Recorded = t0.AddMilliseconds(i),
                };
            }
        }
    }
}

file class SensorReading
{
    [ClickHouseColumn(Name = "id", Order = 0)] public long Id { get; set; }
    [ClickHouseColumn(Name = "sensor", Order = 1)] public string Sensor { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "reading", Order = 2)] public double Reading { get; set; }
    [ClickHouseColumn(Name = "recorded", Order = 3)] public DateTime Recorded { get; set; }
}
