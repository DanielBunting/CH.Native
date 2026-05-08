using CH.Native.Connection;

namespace CH.Native.Samples.Queries;

/// <summary>
/// Log-shaped analytics — the four canonical questions you ask of a request-log table:
/// volume by level, latency by service, error rate by hour, and the slowest tail.
/// Models an oncall dashboard reading from a partitioned <c>MergeTree</c> log table.
/// </summary>
/// <remarks>
/// Pure query showcase — table is seeded server-side via <c>INSERT … SELECT FROM numbers()</c>
/// so the sample stays focused on the read shapes. Each query uses
/// <c>connection.QueryAsync(sql)</c> → <c>IAsyncEnumerable&lt;ClickHouseRow&gt;</c>
/// for schemaless access by column name; swap to <c>QueryAsync&lt;T&gt;</c> if you want
/// POCO mapping. For the bulk-insert side of log ingestion see the
/// <c>long-lived</c> and <c>async-stream</c> samples in <c>CH.Native.Samples.Insert</c>.
/// </remarks>
internal static class LogAnalyticsSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_logs_{Guid.NewGuid():N}";
        const int rowCount = 100_000;

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
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

            // Seed server-side: 100k rows spanning ~28h. Level distribution is weighted
            // INFO-heavy with a ~17% ERROR rate; ERROR rows get a fat-tail latency so the
            // "top slowest" query is dominated by errors, matching production shape.
            await connection.ExecuteNonQueryAsync($"""
                INSERT INTO {tableName}
                SELECT
                    toDateTime('2026-04-01 00:00:00') + number AS timestamp,
                    ['DEBUG', 'INFO', 'INFO', 'INFO', 'WARN', 'ERROR'][1 + number % 6] AS level,
                    ['api-gateway', 'user-service', 'order-service', 'payment-service', 'notification-svc'][1 + intDiv(number, 7) % 5] AS service,
                    ['Request processed', 'Cache hit', 'DB query executed', 'Pool exhausted, retrying',
                     'Timeout waiting downstream', 'Auth token validated', 'Rate limit exceeded',
                     'Health check passed', 'Failed to parse body', 'Unhandled exception'][1 + intDiv(number, 11) % 10] AS message,
                    lower(hex(sipHash64(number))) AS trace_id,
                    if(number % 6 = 5,
                       round(500 + rand(number) % 4500 + (rand(number + 1) % 100) / 100.0, 2),
                       round(1 + rand(number) % 200 + (rand(number + 1) % 100) / 100.0, 2)) AS duration_ms
                FROM numbers({rowCount})
                """);
            Console.WriteLine($"Seeded {tableName} with {rowCount:N0} log entries");

            // Plumbing across every tile: a single CancellationTokenSource for
            // cooperative cancellation, parameterised filters where the dashboard
            // would actually take user input, and an explicit queryId on the
            // hottest tile so the query can be correlated with system.query_log.
            using var cts = new CancellationTokenSource();
            var volumeQueryId = $"log-analytics-volume-{Guid.NewGuid():N}";

            Console.WriteLine("\n--- Log volume by level ---");
            await foreach (var row in connection.QueryAsync(
                $"SELECT level, count() AS cnt FROM {tableName} GROUP BY level ORDER BY cnt DESC",
                cts.Token,
                queryId: volumeQueryId))
            {
                Console.WriteLine($"  {row["level"],-8} {row["cnt"],10:N0}");
            }
            var volumeQueryIdEcho = connection.LastQueryId;

            // Time-window filter on the per-service rollup, anonymous-object
            // params bound server-side.
            var windowStart = new DateTime(2026, 4, 1, 0, 0, 0, DateTimeKind.Utc);
            var windowEnd = windowStart.AddHours(28);

            Console.WriteLine("\n--- Average duration by service (parameterised window) ---");
            await foreach (var row in connection.QueryAsync(
                $"""
                SELECT
                    service,
                    count()                    AS requests,
                    round(avg(duration_ms), 2) AS avg_ms,
                    round(max(duration_ms), 2) AS max_ms
                FROM {tableName}
                WHERE timestamp BETWEEN @windowStart AND @windowEnd
                GROUP BY service
                ORDER BY avg_ms DESC
                """,
                new { windowStart, windowEnd },
                cts.Token))
            {
                Console.WriteLine($"  {row["service"],-16} requests={row["requests"],10:N0}  avg={row["avg_ms"],8}ms  max={row["max_ms"],8}ms");
            }

            // Dictionary-bound params on the error-rate-by-hour tile — the same
            // parameter binding surface but assembled at runtime. ClickHouse
            // does not accept parameters in the LIMIT clause, so the literal
            // stays inline; @errorLevel is bound server-side.
            var hourlyParams = new Dictionary<string, object?>
            {
                ["errorLevel"] = "ERROR",
            };

            Console.WriteLine("\n--- Error rate by hour ---");
            await foreach (var row in connection.QueryAsync(
                $"""
                SELECT
                    toStartOfHour(timestamp)                                        AS hour,
                    count()                                                         AS total,
                    countIf(level = @errorLevel)                                    AS errors,
                    round(countIf(level = @errorLevel) * 100.0 / count(), 2)        AS error_pct
                FROM {tableName}
                GROUP BY hour
                ORDER BY hour
                LIMIT 12
                """,
                hourlyParams,
                cts.Token))
            {
                Console.WriteLine($"  {row["hour"]}  total={row["total"],6:N0}  errors={row["errors"],5:N0}  ({row["error_pct"]}%)");
            }

            // Top-N slowest with a parameterised floor — typical "show me only the
            // outliers worth paging on" filter. LIMIT stays a literal because
            // ClickHouse does not accept parameters in that clause.
            var minDurationMs = 500.0;
            const int topN = 10;

            Console.WriteLine($"\n--- Top {topN} slowest requests (>= {minDurationMs}ms) ---");
            await foreach (var row in connection.QueryAsync(
                $"""
                SELECT timestamp, service, level, duration_ms, message
                FROM {tableName}
                WHERE duration_ms >= @minDurationMs
                ORDER BY duration_ms DESC
                LIMIT {topN}
                """,
                new { minDurationMs },
                cts.Token))
            {
                Console.WriteLine($"  {row["timestamp"]}  {row["service"],-16} {row["level"],-5} {row["duration_ms"],8}ms  {row["message"]}");
            }

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  Anon-obj params bound : @windowStart, @windowEnd, @minDurationMs");
            Console.WriteLine($"  Dict params bound     : @errorLevel");
            Console.WriteLine($"  queryId on volume tile: {volumeQueryId}");
            Console.WriteLine($"  queryId echoed        : {volumeQueryIdEcho}");
            Console.WriteLine($"  Cancellation token    : threaded into every QueryAsync above");
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}
