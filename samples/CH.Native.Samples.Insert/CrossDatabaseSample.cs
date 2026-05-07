using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;

namespace CH.Native.Samples.Insert;

/// <summary>
/// Writing to two tables in different databases over a SINGLE connection. Demonstrates
/// qualified <c>database.table</c> names — both via the typed
/// <c>connection.Table&lt;T&gt;("db.table")</c> handle and via the
/// <c>BulkInsertAsync(database, tableName, columnNames, rows)</c> dynamic overload.
/// </summary>
/// <remarks>
/// ClickHouse's native protocol pins a connection to the database supplied at
/// handshake time, so cross-database routing relies on qualified names in the SQL
/// the driver emits. CH.Native renders a qualified string as <c>`db`.`table`</c>
/// (each segment quoted independently), so the server sees the right database for
/// each insert without bouncing the connection. Use the explicit
/// <c>(database, tableName)</c> overload at the callsite when the table name
/// itself legitimately contains a dot.
/// </remarks>
internal static class CrossDatabaseSample
{
    public static async Task RunAsync(string connectionString)
    {
        var dbOrders = $"sample_orders_{Guid.NewGuid():N}";
        var dbInventory = $"sample_inventory_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();
        Console.WriteLine($"Connected — default DB: {connection.Settings.Database}");

        await connection.ExecuteNonQueryAsync($"CREATE DATABASE {dbOrders}");
        await connection.ExecuteNonQueryAsync($"CREATE DATABASE {dbInventory}");

        try
        {
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {dbOrders}.line_items (
                    order_id   UInt64,
                    sku        String,
                    qty        UInt32,
                    unit_price Float64,
                    placed_at  DateTime
                ) ENGINE = MergeTree()
                ORDER BY (order_id, sku)
                """);

            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {dbInventory}.stock_movements (
                    sku         String,
                    warehouse   LowCardinality(String),
                    delta       Int32,
                    recorded_at DateTime
                ) ENGINE = MergeTree()
                ORDER BY (sku, recorded_at)
                """);

            Console.WriteLine($"Created {dbOrders}.line_items and {dbInventory}.stock_movements");

            // -----------------------------------------------------------------
            // (1) Typed insert via the LINQ table handle, qualified database.table.
            // The qualified name routes the INSERT to dbOrders even though the
            // connection's default DB is something else.
            // -----------------------------------------------------------------
            var t0 = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var lineItems = Enumerable.Range(0, 10_000).Select(i => new LineItem
            {
                OrderId = (ulong)(1_000_000 + i),
                Sku = $"sku-{i % 200:000}",
                Qty = (uint)((i % 5) + 1),
                UnitPrice = 9.99 + ((i % 50) * 0.5),
                PlacedAt = t0.AddSeconds(i)
            }).ToList();

            await connection.Table<LineItem>($"{dbOrders}.line_items").InsertAsync(lineItems);
            Console.WriteLine($"(1) Inserted {lineItems.Count:N0} rows into {dbOrders}.line_items");

            // -----------------------------------------------------------------
            // (2) Dynamic insert into the inventory database, on the SAME
            // connection. Demonstrates that schema cache and busy-slot accounting
            // handle a second insert into a different DB cleanly — no connection
            // bounce required. The (database, tableName) overload is used here
            // for explicit clarity.
            // -----------------------------------------------------------------
            var columns = new[] { "sku", "warehouse", "delta", "recorded_at" };
            var rng = new Random(42);
            var warehouses = new[] { "main", "east", "west", "overflow" };
            var movements = Enumerable.Range(0, 5_000).Select(i => new object?[]
            {
                $"sku-{i % 200:000}",
                warehouses[i % warehouses.Length],
                (i % 7 == 0 ? -1 : 1) * rng.Next(1, 10),
                t0.AddMinutes(i)
            });

            await connection.BulkInsertAsync(
                database: dbInventory,
                tableName: "stock_movements",
                columnNames: columns,
                rows: movements);
            Console.WriteLine($"(2) Inserted 5,000 rows into {dbInventory}.stock_movements");

            // -----------------------------------------------------------------
            // Verify with cross-database queries on the same connection.
            // -----------------------------------------------------------------
            var orderRows = await connection.ExecuteScalarAsync<ulong>($"SELECT count() FROM {dbOrders}.line_items");
            var stockRows = await connection.ExecuteScalarAsync<ulong>($"SELECT count() FROM {dbInventory}.stock_movements");
            Console.WriteLine($"\n{dbOrders}.line_items: {orderRows:N0} rows");
            Console.WriteLine($"{dbInventory}.stock_movements: {stockRows:N0} rows");

            Console.WriteLine("\n--- Top 5 SKUs by ordered quantity ---");
            await foreach (var row in connection.QueryAsync(
                $"""
                SELECT sku, sum(qty) AS total_qty
                FROM {dbOrders}.line_items
                GROUP BY sku
                ORDER BY total_qty DESC
                LIMIT 5
                """))
            {
                Console.WriteLine($"  {row["sku"]}: {row["total_qty"]} units");
            }

            Console.WriteLine("\n--- Net stock change per warehouse ---");
            await foreach (var row in connection.QueryAsync(
                $"""
                SELECT warehouse, sum(delta) AS net_delta
                FROM {dbInventory}.stock_movements
                GROUP BY warehouse
                ORDER BY warehouse
                """))
            {
                Console.WriteLine($"  {row["warehouse"]}: {row["net_delta"]:+0;-#}");
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP DATABASE IF EXISTS {dbOrders}");
            await connection.ExecuteNonQueryAsync($"DROP DATABASE IF EXISTS {dbInventory}");
            Console.WriteLine($"\nDropped {dbOrders} and {dbInventory}");
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
