using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;

var connectionString = args.Length > 0 ? args[0] : "Host=localhost;Port=9000";
var productsTable = $"sample_linq_products_{Guid.NewGuid():N}";
var versionsTable = $"sample_linq_versions_{Guid.NewGuid():N}";

await using var connection = new ClickHouseConnection(connectionString);
await connection.OpenAsync();
Console.WriteLine("Connected to ClickHouse");

try
{
    // Create and populate a products table
    await connection.ExecuteNonQueryAsync($"""
        CREATE TABLE {productsTable} (
            id       UInt32,
            name     String,
            category String,
            price    Float64,
            rating   Float32,
            quantity UInt32
        ) ENGINE = MergeTree()
        ORDER BY (id, intHash32(id))
        SAMPLE BY intHash32(id)
        """);

    await connection.ExecuteNonQueryAsync($"""
        INSERT INTO {productsTable} VALUES
            (1,  'Laptop',       'Electronics', 1299.99, 4.5, 50),
            (2,  'Keyboard',     'Electronics', 79.99,   4.2, 200),
            (3,  'Coffee Maker', 'Kitchen',     149.99,  4.7, 75),
            (4,  'Desk Chair',   'Furniture',   399.99,  4.1, 30),
            (5,  'Monitor',      'Electronics', 549.99,  4.6, 80),
            (6,  'Blender',      'Kitchen',     69.99,   3.9, 120),
            (7,  'Bookshelf',    'Furniture',   189.99,  4.3, 45),
            (8,  'Mouse',        'Electronics', 49.99,   4.4, 300),
            (9,  'Toaster',      'Kitchen',     34.99,   4.0, 150),
            (10, 'Standing Desk','Furniture',   599.99,  4.8, 25)
        """);
    Console.WriteLine($"Created and populated {productsTable}");

    // Basic Where + OrderBy + Take
    Console.WriteLine("\n--- Top 3 electronics by price (Where + OrderByDescending + Take) ---");
    var query = connection.Table<Product>(productsTable)
        .Where(p => p.Category == "Electronics")
        .OrderByDescending(p => p.Price)
        .Take(3);

    Console.WriteLine($"  SQL: {query.ToSql()}");

    await foreach (var p in query.AsAsyncEnumerable())
    {
        Console.WriteLine($"  {p.Name,-15} ${p.Price,8:F2}  rating={p.Rating}");
    }

    // Select projection
    Console.WriteLine("\n--- Furniture names and prices (Select projection) ---");
    var projected = connection.Table<Product>(productsTable)
        .Where(p => p.Category == "Furniture")
        .OrderBy(p => p.Price)
        .Select(p => new ProductSummary { Name = p.Name, Price = p.Price });

    Console.WriteLine($"  SQL: {projected.ToSql()}");

    await foreach (var p in projected.AsAsyncEnumerable())
    {
        Console.WriteLine($"  {p.Name}: ${p.Price:F2}");
    }

    // Aggregation with CountAsync
    var kitchenCount = await connection.Table<Product>(productsTable)
        .Where(p => p.Category == "Kitchen")
        .CountAsync();
    Console.WriteLine($"\nKitchen products: {kitchenCount}");

    // FirstAsync
    var topRated = await connection.Table<Product>(productsTable)
        .OrderByDescending(p => p.Rating)
        .FirstAsync();
    Console.WriteLine($"Top rated: {topRated.Name} (rating={topRated.Rating})");

    // Demonstrate FINAL with a ReplacingMergeTree table
    Console.WriteLine("\n--- FINAL modifier (ReplacingMergeTree) ---");
    await connection.ExecuteNonQueryAsync($"""
        CREATE TABLE {versionsTable} (
            id      UInt32,
            name    String,
            version UInt32
        ) ENGINE = ReplacingMergeTree(version)
        ORDER BY id
        """);

    await connection.ExecuteNonQueryAsync($"""
        INSERT INTO {versionsTable} VALUES (1, 'Alpha', 1), (2, 'Beta', 1)
        """);
    await connection.ExecuteNonQueryAsync($"""
        INSERT INTO {versionsTable} VALUES (1, 'Alpha Updated', 2), (3, 'Gamma', 1)
        """);

    var withFinal = connection.Table<VersionedItem>(versionsTable)
        .Final()
        .OrderBy(v => v.Id);

    Console.WriteLine($"  SQL: {withFinal.ToSql()}");

    await foreach (var item in withFinal.AsAsyncEnumerable())
    {
        Console.WriteLine($"  id={item.Id}, name={item.Name}, version={item.Version}");
    }

    // Demonstrate SAMPLE
    Console.WriteLine("\n--- SAMPLE (approximate 50% of rows) ---");
    var sampled = connection.Table<Product>(productsTable)
        .Sample(0.5);

    Console.WriteLine($"  SQL: {sampled.ToSql()}");

    var sampledCount = 0;
    await foreach (var p in sampled.AsAsyncEnumerable())
    {
        Console.WriteLine($"  {p.Name}");
        sampledCount++;
    }
    Console.WriteLine($"  Returned ~{sampledCount} of 10 rows");
}
finally
{
    await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {productsTable}");
    await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {versionsTable}");
    Console.WriteLine($"\nCleaned up tables");
}

public class Product
{
    [ClickHouseColumn(Name = "id")]
    public uint Id { get; set; }

    [ClickHouseColumn(Name = "name")]
    public string Name { get; set; } = "";

    [ClickHouseColumn(Name = "category")]
    public string Category { get; set; } = "";

    [ClickHouseColumn(Name = "price")]
    public double Price { get; set; }

    [ClickHouseColumn(Name = "rating")]
    public float Rating { get; set; }

    [ClickHouseColumn(Name = "quantity")]
    public uint Quantity { get; set; }
}

public class ProductSummary
{
    [ClickHouseColumn(Name = "name")]
    public string Name { get; set; } = "";

    [ClickHouseColumn(Name = "price")]
    public double Price { get; set; }
}

public class VersionedItem
{
    [ClickHouseColumn(Name = "id")]
    public uint Id { get; set; }

    [ClickHouseColumn(Name = "name")]
    public string Name { get; set; } = "";

    [ClickHouseColumn(Name = "version")]
    public uint Version { get; set; }
}
