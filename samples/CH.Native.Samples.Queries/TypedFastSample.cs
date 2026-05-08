using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.Mapping;

namespace CH.Native.Samples.Queries;

/// <summary>
/// <c>connection.QueryTypedAsync&lt;T&gt;(sql)</c> — high-throughput typed streaming.
/// Models a hot-path analytics fan-out reading 100k sensor readings into POCOs and
/// rolling them up by sensor.
/// </summary>
/// <remarks>
/// Same shape as <c>QueryAsync&lt;T&gt;</c> — POCO with <c>[ClickHouseColumn]</c>
/// attributes, <c>await foreach</c> consumption — but the typed pipeline avoids
/// boxing primitives and fuses column reads against the POCO's setters, which
/// matters once result sets get into the tens-of-thousands of rows. <c>T</c> must
/// have a parameterless constructor.
/// </remarks>
internal static class TypedFastSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_telemetry_{Guid.NewGuid():N}";
        const int rows = 100_000;

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    sensor_id   UInt32,
                    observed_at DateTime64(3),
                    value       Float64
                ) ENGINE = MergeTree()
                ORDER BY (sensor_id, observed_at)
                """);

            // Generate 100k rows server-side — cheaper than streaming them in
            // for a perf demo, and keeps the focus on the read path.
            await connection.ExecuteNonQueryAsync($"""
                INSERT INTO {tableName}
                SELECT
                    number % 50 AS sensor_id,
                    toDateTime64('2026-05-01 00:00:00', 3) + number / 1000 AS observed_at,
                    20 + 80 * (intHash32(number) / 4294967295.0) AS value
                FROM numbers({rows})
                """);
            Console.WriteLine($"Seeded {tableName} with {rows:N0} rows");

            var selectSql = $"SELECT sensor_id, observed_at, value FROM {tableName}";

            // Thread a custom queryId and a CancellationToken through the
            // streaming read so users see those knobs are wired up on the
            // hot path too.
            using var cts = new CancellationTokenSource();
            var queryId = $"typed-fast-{Guid.NewGuid():N}";

            var sw = Stopwatch.StartNew();
            var sumByFastPath = 0.0;
            var countFast = 0L;
            await foreach (var reading in connection.QueryTypedAsync<Reading>(
                selectSql,
                cts.Token,
                queryId: queryId))
            {
                sumByFastPath += reading.Value;
                countFast++;
            }
            sw.Stop();
            var fastMs = sw.Elapsed.TotalMilliseconds;

            // Capture LastQueryId immediately — any later call (like the
            // sanity-check scalar below) would overwrite it with its own id.
            var queryIdEcho = connection.LastQueryId;

            Console.WriteLine();
            Console.WriteLine("--- QueryTypedAsync<T> hot-path read ---");
            Console.WriteLine($"  Rows materialised : {countFast:N0}");
            Console.WriteLine($"  Sum of value      : {sumByFastPath:N1}");
            Console.WriteLine($"  Wall time         : {fastMs:F0}ms ({countFast / sw.Elapsed.TotalSeconds:F0} rows/sec)");

            // Verify by running a server-side aggregate against the same data.
            var serverSum = await connection.ExecuteScalarAsync<double>(
                $"SELECT sum(value) FROM {tableName}",
                cancellationToken: cts.Token);
            Console.WriteLine($"  Server-side sum   : {serverSum:N1}  (sanity-check)");

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  queryId sent       : {queryId}");
            Console.WriteLine($"  queryId echoed     : {queryIdEcho}");
            Console.WriteLine($"  Cancellation token : threaded via cts.Token (not signalled)");
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

file class Reading
{
    [ClickHouseColumn(Name = "sensor_id")] public uint SensorId { get; set; }
    [ClickHouseColumn(Name = "observed_at")] public DateTime ObservedAt { get; set; }
    [ClickHouseColumn(Name = "value")] public double Value { get; set; }
}
