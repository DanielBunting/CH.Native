using CH.Native.Connection;
using CH.Native.Data;

namespace CH.Native.Samples.Queries;

/// <summary>
/// <c>connection.ExecuteScalarAsync&lt;T&gt;(sql)</c> — single-value reads. Models a
/// dashboard tile fan-out where each KPI is one server-side aggregate returning a
/// single scalar of a known type.
/// </summary>
/// <remarks>
/// The fastest read shape — no row materialisation, no enumerator allocation, just
/// the first column of the first row of the result mapped to <typeparamref>T</typeparamref>.
/// Returns <c>default</c> if the result is empty. Reach for it whenever you need
/// a single value: counts, sums, max/min, version strings, status flags.
/// </remarks>
internal static class ScalarSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_orders_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    order_id        UInt64,
                    customer_id     UInt64,
                    total           Decimal64(2),
                    fulfilment_ms   UInt32,
                    placed_at       DateTime
                ) ENGINE = MergeTree()
                ORDER BY (placed_at, order_id)
                """);

            // Seed a small but realistic order book: 10k orders across ~2k distinct
            // customers with random totals and fulfilment latencies.
            await connection.ExecuteNonQueryAsync($"""
                INSERT INTO {tableName}
                SELECT
                    number AS order_id,
                    intDiv(number, 5) % 2000 AS customer_id,
                    toDecimal64(round(15 + rand() % 50000 / 100.0, 2), 2) AS total,
                    50 + rand() % 1500 AS fulfilment_ms,
                    toDateTime('2026-04-01 00:00:00') + number AS placed_at
                FROM numbers(10000)
                """);
            Console.WriteLine($"Seeded {tableName} with 10,000 orders");

            // Each query threads through the full surface: a custom queryId for
            // tracing (echoed back via connection.LastQueryId), a CancellationToken
            // for cooperative cancellation, an IProgress<QueryProgress> for the
            // scalar-revenue tile, and the parameterised extension overload for
            // the per-customer aggregate.
            using var cts = new CancellationTokenSource();
            var revenueQueryId = $"scalar-revenue-{Guid.NewGuid():N}";
            // Track the high-water mark — Progress<T> can deliver callbacks
            // out of order on a thread-pool sync context, and the server may
            // push a final empty progress frame once the stream ends.
            QueryProgress peakProgress = default;
            var progressEvents = 0;
            var progress = new Progress<QueryProgress>(p =>
            {
                progressEvents++;
                if (p.RowsRead > peakProgress.RowsRead) peakProgress = p;
            });

            var totalRevenue = await connection.ExecuteScalarAsync<decimal>(
                $"SELECT sum(total) FROM {tableName}",
                progress: progress,
                cancellationToken: cts.Token,
                queryId: revenueQueryId);
            var revenueQueryIdEcho = connection.LastQueryId;

            var maxOrder = await connection.ExecuteScalarAsync<decimal>(
                $"SELECT max(total) FROM {tableName}",
                cancellationToken: cts.Token,
                queryId: $"scalar-max-{Guid.NewGuid():N}");

            var distinctCustomers = await connection.ExecuteScalarAsync<ulong>(
                $"SELECT uniqExact(customer_id) FROM {tableName}",
                cancellationToken: cts.Token);

            // Parameterised scalar — extension overload, anonymous-object params.
            // Demonstrates that params bind through the same surface.
            var minTotal = 100m;
            var ordersAboveThreshold = await connection.ExecuteScalarAsync<ulong>(
                $"SELECT count() FROM {tableName} WHERE total > @minTotal",
                new { minTotal },
                cts.Token);

            var p95FulfilmentMs = await connection.ExecuteScalarAsync<double>(
                $"SELECT quantile(0.95)(fulfilment_ms) FROM {tableName}",
                cancellationToken: cts.Token);

            Console.WriteLine();
            Console.WriteLine("--- Order analytics dashboard ---");
            Console.WriteLine($"  Total revenue                : {totalRevenue,12:N2}");
            Console.WriteLine($"  Max order value              : {maxOrder,12:N2}");
            Console.WriteLine($"  Distinct customers           : {distinctCustomers,12:N0}");
            Console.WriteLine($"  Orders > {minTotal:C} (parameterised) : {ordersAboveThreshold,12:N0}");
            Console.WriteLine($"  p95 fulfilment (ms)          : {p95FulfilmentMs,12:F1}");

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  queryId sent       : {revenueQueryId}");
            Console.WriteLine($"  queryId echoed     : {revenueQueryIdEcho}");
            Console.WriteLine($"  IProgress fired    : {progressEvents} event(s), peak RowsRead={peakProgress.RowsRead:N0}, BytesRead={peakProgress.BytesRead:N0}");
            Console.WriteLine($"  Cancellation token : threaded via cts.Token (not signalled)");
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}
