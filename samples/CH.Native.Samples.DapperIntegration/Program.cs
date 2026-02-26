using CH.Native.Ado;
using CH.Native.Connection;
using Dapper;

// Dapper maps columns to properties by name — enable underscore matching
// so that e.g. "in_stock" maps to the InStock property.
Dapper.DefaultTypeMap.MatchNamesWithUnderscores = true;

var connectionString = args.Length > 0 ? args[0] : "Host=localhost;Port=9000";
var tableName = $"sample_products_{Guid.NewGuid():N}";

await using var connection = new ClickHouseDbConnection(connectionString);
await connection.OpenAsync();
Console.WriteLine("Connected to ClickHouse via ADO.NET");

try
{
    // Create table using Dapper's ExecuteAsync
    await connection.ExecuteAsync($"""
        CREATE TABLE {tableName} (
            id       UInt32,
            name     String,
            category String,
            price    Float64,
            in_stock UInt8
        ) ENGINE = MergeTree()
        ORDER BY id
        """);
    Console.WriteLine($"Created table {tableName}");

    // Insert data
    await connection.ExecuteAsync($"""
        INSERT INTO {tableName} VALUES
            (1, 'Laptop',      'Electronics', 999.99, 1),
            (2, 'Keyboard',    'Electronics', 79.99,  1),
            (3, 'Coffee Mug',  'Kitchen',     12.99,  1),
            (4, 'Desk Lamp',   'Office',      45.50,  0),
            (5, 'Headphones',  'Electronics', 199.99, 1),
            (6, 'Notebook',    'Office',      5.99,   1),
            (7, 'Water Bottle','Kitchen',     24.99,  1)
        """);
    Console.WriteLine("Inserted 7 products");

    // Query all products with Dapper
    Console.WriteLine("\n--- All products (Dapper QueryAsync<T>) ---");
    var products = await connection.QueryAsync<Product>(
        $"SELECT id, name, category, price, in_stock FROM {tableName} ORDER BY id");

    foreach (var p in products)
    {
        Console.WriteLine($"  [{p.Id}] {p.Name,-15} {p.Category,-12} ${p.Price,8:F2}  {(p.InStock == 1 ? "In Stock" : "Out of Stock")}");
    }

    // Parameterized query
    Console.WriteLine("\n--- Electronics over $100 (parameterized) ---");
    var expensive = await connection.QueryAsync<Product>(
        $"SELECT id, name, category, price, in_stock FROM {tableName} WHERE category = @cat AND price > @minPrice ORDER BY price DESC",
        new { cat = "Electronics", minPrice = 100.0 });

    foreach (var p in expensive)
    {
        Console.WriteLine($"  {p.Name}: ${p.Price:F2}");
    }

    // Single result with QueryFirstAsync
    var cheapest = await connection.QueryFirstAsync<Product>(
        $"SELECT id, name, category, price, in_stock FROM {tableName} ORDER BY price ASC LIMIT 1");
    Console.WriteLine($"\nCheapest product: {cheapest.Name} at ${cheapest.Price:F2}");

    // Scalar query
    var avgPrice = await connection.ExecuteScalarAsync<double>(
        $"SELECT round(avg(price), 2) FROM {tableName}");
    Console.WriteLine($"Average price: ${avgPrice:F2}");

    // Use a native ClickHouseConnection directly for advanced features
    Console.WriteLine("\n--- Using native ClickHouseConnection ---");
    await using var native = new ClickHouseConnection(connectionString);
    await native.OpenAsync();
    await foreach (var row in native.QueryAsync(
        $"SELECT category, count() AS cnt, round(avg(price), 2) AS avg_price FROM {tableName} GROUP BY category ORDER BY cnt DESC"))
    {
        Console.WriteLine($"  {row["category"]}: {row["cnt"]} products, avg ${row["avg_price"]}");
    }
}
finally
{
    await connection.ExecuteAsync($"DROP TABLE IF EXISTS {tableName}");
    Console.WriteLine($"\nDropped table {tableName}");
}

// Dapper maps by column name, not ClickHouseColumnAttribute.
// With MatchNamesWithUnderscores = true, "in_stock" maps to InStock.
public class Product
{
    public uint Id { get; set; }
    public string Name { get; set; } = "";
    public string Category { get; set; } = "";
    public double Price { get; set; }
    public byte InStock { get; set; }
}
