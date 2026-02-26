using System.Diagnostics;
using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;

var connectionString = args.Length > 0 ? args[0] : "Host=localhost;Port=9000";
var tableName = $"sample_sensor_readings_{Guid.NewGuid():N}";
const int rowCount = 100_000;

await using var connection = new ClickHouseConnection(connectionString);
await connection.OpenAsync();
Console.WriteLine("Connected to ClickHouse");

try
{
    // Create a partitioned MergeTree table
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
    Console.WriteLine($"Created table {tableName}");

    // Generate synthetic data
    var random = new Random(42);
    var baseTime = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    var readings = new List<SensorReading>(rowCount);

    for (var i = 0; i < rowCount; i++)
    {
        readings.Add(new SensorReading
        {
            SensorId = (uint)(i % 100),
            Timestamp = baseTime.AddSeconds(i * 30),
            Temperature = 20.0 + random.NextDouble() * 15.0,
            Humidity = 40.0 + random.NextDouble() * 40.0,
            Pressure = 1000.0 + random.NextDouble() * 50.0
        });
    }

    // Bulk insert with batching
    var sw = Stopwatch.StartNew();
    var options = new BulkInsertOptions { BatchSize = 5000 };
    await using var inserter = connection.CreateBulkInserter<SensorReading>(tableName, options);
    await inserter.InitAsync();
    await inserter.AddRangeAsync(readings);
    await inserter.CompleteAsync();
    sw.Stop();

    Console.WriteLine($"Inserted {rowCount:N0} rows in {sw.Elapsed.TotalMilliseconds:F0}ms " +
                      $"({rowCount / sw.Elapsed.TotalSeconds:F0} rows/sec)");

    // Verify with aggregation queries
    var totalRows = await connection.ExecuteScalarAsync<ulong>($"SELECT count() FROM {tableName}");
    Console.WriteLine($"\nTotal rows: {totalRows:N0}");

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

public class SensorReading
{
    [ClickHouseColumn(Name = "sensor_id", Order = 0)]
    public uint SensorId { get; set; }

    [ClickHouseColumn(Name = "timestamp", Order = 1)]
    public DateTime Timestamp { get; set; }

    [ClickHouseColumn(Name = "temperature", Order = 2)]
    public double Temperature { get; set; }

    [ClickHouseColumn(Name = "humidity", Order = 3)]
    public double Humidity { get; set; }

    [ClickHouseColumn(Name = "pressure", Order = 4)]
    public double Pressure { get; set; }
}
