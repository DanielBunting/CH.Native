using System.Diagnostics;
using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;

var connectionString = args.Length > 0 ? args[0] : "Host=localhost;Port=9000";
var tableName = $"sample_logs_{Guid.NewGuid():N}";
const int rowCount = 500_000;

await using var connection = new ClickHouseConnection(connectionString);
await connection.OpenAsync();
Console.WriteLine("Connected to ClickHouse");

try
{
    // Create a log table partitioned by month
    await connection.ExecuteNonQueryAsync($"""
        CREATE TABLE {tableName} (
            timestamp   DateTime,
            level       LowCardinality(String),
            service     LowCardinality(String),
            message     String,
            trace_id    String,
            duration_ms Float64
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (service, timestamp)
        """);
    Console.WriteLine($"Created table {tableName}");

    // Generate and bulk insert 500k log entries using CreateBulkInserter
    Console.WriteLine($"Inserting {rowCount:N0} log entries...");
    var sw = Stopwatch.StartNew();

    await using (var inserter = connection.CreateBulkInserter<LogEntry>(tableName, new BulkInsertOptions { BatchSize = 10_000 }))
    {
        await inserter.InitAsync();
        await inserter.AddRangeStreamingAsync(GenerateLogs(rowCount));
        await inserter.CompleteAsync();
    }

    sw.Stop();
    Console.WriteLine($"Inserted {rowCount:N0} rows in {sw.Elapsed.TotalSeconds:F1}s " +
                      $"({rowCount / sw.Elapsed.TotalSeconds:F0} rows/sec)");

    // Analytical queries
    Console.WriteLine("\n=== Log count by level ===");
    await foreach (var row in connection.QueryAsync(
        $"SELECT level, count() AS cnt FROM {tableName} GROUP BY level ORDER BY cnt DESC"))
    {
        Console.WriteLine($"  {row["level"],-8} {row["cnt"],10:N0}");
    }

    Console.WriteLine("\n=== Average duration by service ===");
    await foreach (var row in connection.QueryAsync(
        $"""
        SELECT
            service,
            count()                    AS requests,
            round(avg(duration_ms), 2) AS avg_ms,
            round(max(duration_ms), 2) AS max_ms
        FROM {tableName}
        GROUP BY service
        ORDER BY avg_ms DESC
        """))
    {
        Console.WriteLine($"  {row["service"],-16} requests={row["requests"],10:N0}  avg={row["avg_ms"],8}ms  max={row["max_ms"],8}ms");
    }

    Console.WriteLine("\n=== Error rate by hour ===");
    await foreach (var row in connection.QueryAsync(
        $"""
        SELECT
            toStartOfHour(timestamp)                                    AS hour,
            count()                                                     AS total,
            countIf(level = 'ERROR')                                    AS errors,
            round(countIf(level = 'ERROR') * 100.0 / count(), 2)       AS error_pct
        FROM {tableName}
        GROUP BY hour
        ORDER BY hour
        LIMIT 12
        """))
    {
        Console.WriteLine($"  {row["hour"]}  total={row["total"],8:N0}  errors={row["errors"],6:N0}  ({row["error_pct"]}%)");
    }

    Console.WriteLine("\n=== Top 10 slowest requests ===");
    await foreach (var row in connection.QueryAsync(
        $"""
        SELECT timestamp, service, level, duration_ms, message
        FROM {tableName}
        ORDER BY duration_ms DESC
        LIMIT 10
        """))
    {
        Console.WriteLine($"  {row["timestamp"]}  {row["service"],-16} {row["level"],-8} {row["duration_ms"],8:F1}ms  {row["message"]}");
    }
}
finally
{
    await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
    Console.WriteLine($"\nDropped table {tableName}");
}

static async IAsyncEnumerable<LogEntry> GenerateLogs(int count)
{
    var random = new Random(42);
    var services = new[] { "api-gateway", "user-service", "order-service", "payment-service", "notification-svc" };
    var levels = new[] { "DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR" }; // weighted towards INFO
    var messages = new[]
    {
        "Request processed successfully",
        "Cache hit for key",
        "Database query executed",
        "Connection pool exhausted, retrying",
        "Timeout waiting for downstream service",
        "Authentication token validated",
        "Rate limit exceeded for client",
        "Health check passed",
        "Failed to parse request body",
        "Unhandled exception in request pipeline"
    };

    var baseTime = new DateTime(2024, 6, 1, 0, 0, 0, DateTimeKind.Utc);

    for (var i = 0; i < count; i++)
    {
        var level = levels[random.Next(levels.Length)];
        var durationMs = level == "ERROR"
            ? 500.0 + random.NextDouble() * 4500.0
            : 1.0 + random.NextDouble() * 200.0;

        yield return new LogEntry
        {
            Timestamp = baseTime.AddMilliseconds(i * 50 + random.Next(50)),
            Level = level,
            Service = services[random.Next(services.Length)],
            Message = messages[random.Next(messages.Length)],
            TraceId = Guid.NewGuid().ToString("N"),
            DurationMs = Math.Round(durationMs, 2)
        };

        if (i % 100_000 == 0)
            await Task.Yield();
    }
}

public class LogEntry
{
    [ClickHouseColumn(Name = "timestamp", Order = 0)]
    public DateTime Timestamp { get; set; }

    [ClickHouseColumn(Name = "level", Order = 1)]
    public string Level { get; set; } = "";

    [ClickHouseColumn(Name = "service", Order = 2)]
    public string Service { get; set; } = "";

    [ClickHouseColumn(Name = "message", Order = 3)]
    public string Message { get; set; } = "";

    [ClickHouseColumn(Name = "trace_id", Order = 4)]
    public string TraceId { get; set; } = "";

    [ClickHouseColumn(Name = "duration_ms", Order = 5)]
    public double DurationMs { get; set; }
}
