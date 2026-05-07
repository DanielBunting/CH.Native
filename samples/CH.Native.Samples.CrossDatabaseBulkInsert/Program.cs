using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;

// Demonstrates writing to two tables in different databases over a SINGLE
// connection. ClickHouse's native protocol pins a connection to the database
// supplied at handshake time, so cross-database routing relies on qualified
// `database.table` names in the SQL we emit. CH.Native renders qualified names
// as `` `db`.`table` `` (each segment quoted independently), so the server sees
// the right database for each insert.

var connectionString = args.Length > 0 ? args[0] : "Host=localhost;Port=9000";

// Two databases that won't collide with anything else on the cluster. The
// connection's *default* database stays whatever the connection string set —
// the qualified table names override it per insert.
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

    // ---------------------------------------------------------------------
    // Insert #1 — POCO bulk insert into the "orders" database.
    // The qualified name in `tableName` routes the INSERT to dbOrders even
    // though the connection's default DB is something else.
    // ---------------------------------------------------------------------
    var lineItems = Enumerable.Range(0, 10_000)
        .Select(i => new LineItem
        {
            OrderId = (ulong)(1_000_000 + i),
            Sku = $"sku-{i % 200:000}",
            Qty = (uint)((i % 5) + 1),
            UnitPrice = 9.99 + ((i % 50) * 0.5),
            PlacedAt = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc).AddSeconds(i)
        })
        .ToList();

    await connection.BulkInsertAsync<LineItem>($"{dbOrders}.line_items", lineItems);
    Console.WriteLine($"Inserted {lineItems.Count:N0} rows into {dbOrders}.line_items");

    // ---------------------------------------------------------------------
    // Insert #2 — DynamicBulkInserter (no POCO) into the "inventory"
    // database, on the SAME connection. Demonstrates that the schema cache
    // and busy-slot accounting handle a second insert into a different DB
    // cleanly — no connection bounce required.
    //
    // The (database, tableName) overload is used here purely for clarity at
    // the callsite; passing $"{dbInventory}.stock_movements" as a single
    // qualified string is equivalent.
    // ---------------------------------------------------------------------
    var columns = new[] { "sku", "warehouse", "delta", "recorded_at" };
    var rng = new Random(42);
    var warehouses = new[] { "main", "east", "west", "overflow" };
    var movements = Enumerable.Range(0, 5_000)
        .Select(i => new object?[]
        {
            $"sku-{i % 200:000}",
            warehouses[i % warehouses.Length],
            (i % 7 == 0 ? -1 : 1) * rng.Next(1, 10),
            new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc).AddMinutes(i)
        });

    await connection.BulkInsertAsync(
        database: dbInventory,
        tableName: "stock_movements",
        columnNames: columns,
        rows: movements);
    Console.WriteLine($"Inserted 5,000 rows into {dbInventory}.stock_movements");

    // ---------------------------------------------------------------------
    // Verify with cross-database queries on the same connection.
    // ---------------------------------------------------------------------
    var orderRows = await connection.ExecuteScalarAsync<ulong>(
        $"SELECT count() FROM {dbOrders}.line_items");
    var stockRows = await connection.ExecuteScalarAsync<ulong>(
        $"SELECT count() FROM {dbInventory}.stock_movements");
    Console.WriteLine($"\n{dbOrders}.line_items has {orderRows:N0} rows");
    Console.WriteLine($"{dbInventory}.stock_movements has {stockRows:N0} rows");

    Console.WriteLine("\n--- Top 5 SKUs by quantity (orders DB) ---");
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

    Console.WriteLine("\n--- Net stock change per warehouse (inventory DB) ---");
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

public class LineItem
{
    [ClickHouseColumn(Name = "order_id", Order = 0)]
    public ulong OrderId { get; set; }

    [ClickHouseColumn(Name = "sku", Order = 1)]
    public string Sku { get; set; } = string.Empty;

    [ClickHouseColumn(Name = "qty", Order = 2)]
    public uint Qty { get; set; }

    [ClickHouseColumn(Name = "unit_price", Order = 3)]
    public double UnitPrice { get; set; }

    [ClickHouseColumn(Name = "placed_at", Order = 4)]
    public DateTime PlacedAt { get; set; }
}
