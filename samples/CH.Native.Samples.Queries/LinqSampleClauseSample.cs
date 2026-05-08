using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;

namespace CH.Native.Samples.Queries;

/// <summary>
/// <c>connection.Table&lt;T&gt;(name).Sample(0.1).WithQueryId(...)</c> — the SAMPLE
/// modifier for approximate analytics, paired with a custom query id for tracing.
/// Models a revenue rollup over a 100k-row event table where a 10% sample is
/// good enough.
/// </summary>
/// <remarks>
/// SAMPLE only does anything for engines declared with <c>SAMPLE BY ...</c>. The
/// engine deterministically picks rows whose hashed sample key falls in the
/// requested fraction — same fraction → same rows, run-over-run. WithQueryId
/// attaches a tracing id so the query can be correlated with
/// <c>system.query_log</c>.
/// </remarks>
internal static class LinqSampleClauseSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_events_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            // SAMPLE BY requires the sample key to be in the ORDER BY tuple.
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    event_id UInt64,
                    user_id  UInt64,
                    revenue  Float64
                ) ENGINE = MergeTree()
                ORDER BY (user_id, intHash64(user_id))
                SAMPLE BY intHash64(user_id)
                """);

            await connection.ExecuteNonQueryAsync($"""
                INSERT INTO {tableName}
                SELECT
                    number AS event_id,
                    number % 5000 AS user_id,
                    round(0.5 + (intHash32(number) / 4294967295.0) * 99.5, 2) AS revenue
                FROM numbers(100000)
                """);
            Console.WriteLine($"Seeded {tableName} with 100,000 events");

            using var cts = new CancellationTokenSource();
            var queryId = $"linq-sample-rev-rollup-{Guid.NewGuid():N}";
            var fraction = 0.1;

            var sampled = connection.Table<EventRow>(tableName)
                .Sample(fraction)
                .WithQueryId(queryId);

            Console.WriteLine();
            Console.WriteLine($"--- SAMPLE {fraction:P0} of events ---");
            Console.WriteLine($"SQL: {sampled.ToSql()}");

            var sampleSum = 0.0;
            var sampleCount = 0;
            await foreach (var e in sampled.AsAsyncEnumerable().WithCancellation(cts.Token))
            {
                sampleSum += e.Revenue;
                sampleCount++;
            }
            var sampledQueryIdEcho = connection.LastQueryId;

            // Compare against the full server-side aggregate so users can see
            // the approximation is reasonable.
            var fullSum = await connection.ExecuteScalarAsync<double>(
                $"SELECT sum(revenue) FROM {tableName}",
                cancellationToken: cts.Token);

            // The SAMPLE-fraction extrapolation: scale the sampled sum back up.
            var extrapolated = sampleSum / fraction;

            Console.WriteLine($"  Sampled rows         : {sampleCount:N0}");
            Console.WriteLine($"  Sampled revenue      : {sampleSum:N2}");
            Console.WriteLine($"  Extrapolated revenue : {extrapolated:N2}  (sampled / {fraction:P0})");
            Console.WriteLine($"  Actual revenue       : {fullSum:N2}");
            Console.WriteLine($"  Relative error       : {(extrapolated - fullSum) / fullSum:P2}");

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  Sample fraction         : {fraction}");
            Console.WriteLine($"  queryId via WithQueryId : {queryId}");
            Console.WriteLine($"  queryId echoed          : {sampledQueryIdEcho}");
            Console.WriteLine($"  Cancellation token      : threaded via WithCancellation");
            Console.WriteLine($"  Tip: SELECT * FROM system.query_log WHERE query_id = '{queryId}' to see server-side telemetry");
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

file class EventRow
{
    [ClickHouseColumn(Name = "event_id")] public ulong EventId { get; set; }
    [ClickHouseColumn(Name = "user_id")] public ulong UserId { get; set; }
    [ClickHouseColumn(Name = "revenue")] public double Revenue { get; set; }
}
