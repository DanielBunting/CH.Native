using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;

namespace CH.Native.Samples.Insert;

/// <summary>
/// <c>connection.Table&lt;T&gt;(name).InsertAsync(IAsyncEnumerable&lt;T&gt; rows)</c> —
/// streaming source with bounded memory. Models pulling event pages from a paginated
/// upstream API and landing them as they arrive.
/// </summary>
/// <remarks>
/// Same one-INSERT-many-rows shape as the IEnumerable&lt;T&gt; overload, but the
/// source is async. Memory stays bounded by <c>BulkInsertOptions.BatchSize</c>:
/// the driver fills a batch, sends it, and pulls the next page lazily — useful
/// when the total size is open-ended or you don't want to materialize the
/// entire result set up front.
/// </remarks>
internal static class AsyncStreamSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_events_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    occurred_at DateTime,
                    event_type  LowCardinality(String),
                    user_id     UInt64,
                    payload     String
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(occurred_at)
                ORDER BY (event_type, occurred_at)
                """);
            Console.WriteLine($"Created events table {tableName}");

            var sw = Stopwatch.StartNew();
            await connection.Table<EventRow>(tableName).InsertAsync(StreamPagesAsync());
            sw.Stop();

            // Local iterator function — keeps the EventRow file-local type out of any
            // class-level member signature (which would conflict with file-class
            // visibility rules). Simulates a paginated upstream — 10 pages of 1,000
            // events each, with a small per-page wait so the IAsyncEnumerable<T>
            // path is exercised rather than degenerating to a synchronous loop.
            async IAsyncEnumerable<EventRow> StreamPagesAsync()
            {
                var rng = new Random(11);
                var eventTypes = new[] { "click", "view", "purchase", "scroll", "signup" };
                var t0 = new DateTime(2026, 5, 1, 0, 0, 0, DateTimeKind.Utc);

                for (var page = 0; page < 10; page++)
                {
                    await Task.Delay(5);

                    for (var i = 0; i < 1_000; i++)
                    {
                        var idx = page * 1_000 + i;
                        yield return new EventRow
                        {
                            OccurredAt = t0.AddSeconds(idx),
                            EventType = eventTypes[rng.Next(eventTypes.Length)],
                            UserId = (ulong)(10_000 + rng.Next(0, 500)),
                            Payload = $"page={page};seq={i}"
                        };
                    }

                    Console.WriteLine($"  page {page + 1}/10 fetched");
                }
            }

            var total = await connection.ExecuteScalarAsync<ulong>($"SELECT count() FROM {tableName}");
            Console.WriteLine($"Streamed {total:N0} rows across paginated source in " +
                              $"{sw.Elapsed.TotalMilliseconds:F0}ms.");

            Console.WriteLine("\n--- Counts by event_type ---");
            await foreach (var row in connection.QueryAsync(
                $"SELECT event_type, count() AS n FROM {tableName} GROUP BY event_type ORDER BY n DESC"))
            {
                Console.WriteLine($"  {row["event_type"]}: {row["n"]:N0}");
            }
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
    [ClickHouseColumn(Name = "occurred_at", Order = 0)] public DateTime OccurredAt { get; set; }
    [ClickHouseColumn(Name = "event_type", Order = 1)] public string EventType { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "user_id", Order = 2)] public ulong UserId { get; set; }
    [ClickHouseColumn(Name = "payload", Order = 3)] public string Payload { get; set; } = string.Empty;
}
