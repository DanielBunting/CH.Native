using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;

namespace CH.Native.Samples.Queries;

/// <summary>
/// <c>connection.Table&lt;T&gt;(name).Final()</c> — the FINAL modifier for engines
/// that collapse rows on merge (<c>ReplacingMergeTree</c>, <c>CollapsingMergeTree</c>,
/// <c>VersionedCollapsingMergeTree</c>). Models a current-state read against an
/// inventory ledger that has been overwritten by later versions.
/// </summary>
/// <remarks>
/// Without FINAL the read returns every stored row, including superseded ones —
/// fast, but not what you want when you need the latest version per key. FINAL
/// runs the merge logic at read time, returning one row per ORDER BY key. It is
/// significantly more expensive than a normal SELECT, so reach for it only
/// when current-state semantics matter.
/// </remarks>
internal static class LinqFinalSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_inventory_versions_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    sku     String,
                    name    String,
                    price   Float64,
                    version UInt64
                ) ENGINE = ReplacingMergeTree(version)
                ORDER BY sku
                """);

            // Seed with two versions of two SKUs plus one fresh SKU. The
            // ReplacingMergeTree keeps both versions on disk until merge runs;
            // FINAL collapses them at read time.
            await connection.ExecuteNonQueryAsync($"""
                INSERT INTO {tableName} VALUES
                    ('sku-001', 'Laptop',  1299.00, 1),
                    ('sku-002', 'Keyboard',  79.00, 1)
                """);
            await connection.ExecuteNonQueryAsync($"""
                INSERT INTO {tableName} VALUES
                    ('sku-001', 'Laptop Pro', 1499.00, 2),
                    ('sku-002', 'Keyboard',     69.00, 2),
                    ('sku-003', 'Mouse',        49.00, 1)
                """);
            Console.WriteLine($"Seeded {tableName} with two versions of sku-001 and sku-002");

            using var cts = new CancellationTokenSource();
            var queryId = $"linq-final-{Guid.NewGuid():N}";

            // Without FINAL — every stored row, including superseded versions.
            var allVersions = connection.Table<InventoryVersion>(tableName).OrderBy(v => v.Sku).ThenBy(v => v.Version);
            Console.WriteLine();
            Console.WriteLine("--- Without FINAL (every stored row) ---");
            Console.WriteLine($"SQL: {allVersions.ToSql()}");
            await foreach (var v in allVersions.AsAsyncEnumerable().WithCancellation(cts.Token))
            {
                Console.WriteLine($"  {v.Sku}  v{v.Version}  {v.Name,-12} ${v.Price,8:F2}");
            }

            // With FINAL — one row per sku, post-merge. WithQueryId attaches a
            // tracing id we can echo back via connection.LastQueryId.
            var current = connection.Table<InventoryVersion>(tableName)
                .Final()
                .OrderBy(v => v.Sku)
                .WithQueryId(queryId);

            Console.WriteLine();
            Console.WriteLine("--- With FINAL (current state) ---");
            Console.WriteLine($"SQL: {current.ToSql()}");
            await foreach (var v in current.AsAsyncEnumerable().WithCancellation(cts.Token))
            {
                Console.WriteLine($"  {v.Sku}  v{v.Version}  {v.Name,-12} ${v.Price,8:F2}");
            }

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  queryId via WithQueryId : {queryId}");
            Console.WriteLine($"  queryId echoed          : {connection.LastQueryId}");
            Console.WriteLine($"  Cancellation token      : threaded via WithCancellation on AsAsyncEnumerable");
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

file class InventoryVersion
{
    [ClickHouseColumn(Name = "sku")] public string Sku { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "name")] public string Name { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "price")] public double Price { get; set; }
    [ClickHouseColumn(Name = "version")] public ulong Version { get; set; }
}
