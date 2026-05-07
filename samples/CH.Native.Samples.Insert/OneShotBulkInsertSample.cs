using System.Diagnostics;
using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;

namespace CH.Native.Samples.Insert;

/// <summary>
/// <c>connection.BulkInsertAsync&lt;T&gt;(tableName, rows, options)</c> — terse
/// direct API. Models a sensor-telemetry pipeline pushing 100k readings into a
/// partitioned MergeTree table.
/// </summary>
/// <remarks>
/// Functionally equivalent to <c>connection.Table&lt;T&gt;(name).InsertAsync(rows)</c>:
/// same underlying lifecycle and same options surface. Pick this when you don't
/// need a queryable handle and just want one method call to land a batch.
/// </remarks>
internal static class OneShotBulkInsertSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_sensors_{Guid.NewGuid():N}";
        const int rowCount = 100_000;

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            // Time-series shape: PARTITION BY toYYYYMM keeps writes localised to the
            // current month-partition; ORDER BY (sensor_id, timestamp) makes per-sensor
            // historical reads sequential.
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    sensor_id   UInt32,
                    timestamp   DateTime,
                    temperature Float64,
                    humidity    Float64,
                    pressure    Float64
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (sensor_id, timestamp)
                """);
            Console.WriteLine($"Created sensor table {tableName}");

            var rng = new Random(42);
            var t0 = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var readings = Enumerable.Range(0, rowCount).Select(i => new SensorReading
            {
                SensorId = (uint)(i % 100),
                Timestamp = t0.AddSeconds(i * 30),
                Temperature = 20.0 + rng.NextDouble() * 15.0,
                Humidity = 40.0 + rng.NextDouble() * 40.0,
                Pressure = 1000.0 + rng.NextDouble() * 50.0
            });

            var sw = Stopwatch.StartNew();
            await connection.BulkInsertAsync(tableName, readings, new BulkInsertOptions { BatchSize = 5_000 });
            sw.Stop();

            Console.WriteLine($"Inserted {rowCount:N0} rows in {sw.Elapsed.TotalMilliseconds:F0}ms " +
                              $"({rowCount / sw.Elapsed.TotalSeconds:F0} rows/sec).");

            Console.WriteLine("\n--- Average readings by sensor (first 5) ---");
            await foreach (var row in connection.QueryAsync(
                $"""
                SELECT
                    sensor_id,
                    round(avg(temperature), 2) AS avg_temp,
                    round(avg(humidity), 2)    AS avg_humidity,
                    round(avg(pressure), 2)    AS avg_pressure
                FROM {tableName}
                GROUP BY sensor_id
                ORDER BY sensor_id
                LIMIT 5
                """))
            {
                Console.WriteLine($"  Sensor {row["sensor_id"]}: " +
                                  $"temp={row["avg_temp"]}°C, " +
                                  $"humidity={row["avg_humidity"]}%, " +
                                  $"pressure={row["avg_pressure"]} hPa");
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

file class SensorReading
{
    [ClickHouseColumn(Name = "sensor_id", Order = 0)] public uint SensorId { get; set; }
    [ClickHouseColumn(Name = "timestamp", Order = 1)] public DateTime Timestamp { get; set; }
    [ClickHouseColumn(Name = "temperature", Order = 2)] public double Temperature { get; set; }
    [ClickHouseColumn(Name = "humidity", Order = 3)] public double Humidity { get; set; }
    [ClickHouseColumn(Name = "pressure", Order = 4)] public double Pressure { get; set; }
}
