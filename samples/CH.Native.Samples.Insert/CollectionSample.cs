using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;

namespace CH.Native.Samples.Insert;

/// <summary>
/// <c>connection.Table&lt;T&gt;(name).InsertAsync(IEnumerable&lt;T&gt; rows)</c> — the
/// mainstream ergonomic. Models a checkout pipeline writing a batch of order line
/// items to an analytics table.
/// </summary>
/// <remarks>
/// One INSERT context on the wire, many rows over one (or many) data blocks. Schema
/// cache, telemetry, query id, and batch size are all inherited from
/// <c>BulkInsertOptions</c>. Use this any time you have a known set of rows to
/// land at once.
/// </remarks>
internal static class CollectionSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_orders_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            // Order-analytics table: partitioned by month for cheap retention rollover,
            // ordered by (sku, placed_at) so per-SKU revenue queries are sequential reads.
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    order_id   UInt64,
                    sku        String,
                    qty        UInt32,
                    unit_price Float64,
                    placed_at  DateTime
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(placed_at)
                ORDER BY (sku, placed_at)
                """);
            Console.WriteLine($"Created orders table {tableName}");

            var rng = new Random(7);
            var baseTime = new DateTime(2026, 1, 1, 9, 0, 0, DateTimeKind.Utc);
            var rows = Enumerable.Range(0, 10_000).Select(i => new LineItem
            {
                OrderId = (ulong)(1_000_000 + i),
                Sku = $"sku-{rng.Next(0, 200):000}",
                Qty = (uint)rng.Next(1, 6),
                UnitPrice = Math.Round(9.99 + rng.NextDouble() * 90.0, 2),
                PlacedAt = baseTime.AddSeconds(i * 7)
            }).ToList();

            var sw = Stopwatch.StartNew();
            await connection.Table<LineItem>(tableName).InsertAsync(rows);
            sw.Stop();

            Console.WriteLine($"Inserted {rows.Count:N0} rows in {sw.Elapsed.TotalMilliseconds:F0}ms " +
                              $"({rows.Count / sw.Elapsed.TotalSeconds:F0} rows/sec).");

            Console.WriteLine("\n--- Top 5 SKUs by revenue ---");
            await foreach (var row in connection.QueryAsync(
                $"""
                SELECT sku, round(sum(qty * unit_price), 2) AS revenue
                FROM {tableName}
                GROUP BY sku
                ORDER BY revenue DESC
                LIMIT 5
                """))
            {
                Console.WriteLine($"  {row["sku"]}: {row["revenue"]:N2}");
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

file class LineItem
{
    [ClickHouseColumn(Name = "order_id", Order = 0)] public ulong OrderId { get; set; }
    [ClickHouseColumn(Name = "sku", Order = 1)] public string Sku { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "qty", Order = 2)] public uint Qty { get; set; }
    [ClickHouseColumn(Name = "unit_price", Order = 3)] public double UnitPrice { get; set; }
    [ClickHouseColumn(Name = "placed_at", Order = 4)] public DateTime PlacedAt { get; set; }
}
