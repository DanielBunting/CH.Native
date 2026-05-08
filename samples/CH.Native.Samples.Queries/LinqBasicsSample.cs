using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;

namespace CH.Native.Samples.Queries;

/// <summary>
/// <c>connection.Table&lt;T&gt;(name).Where(...).Select(...).OrderBy(...).Take(...)</c>
/// — compositional LINQ query building. Models a catalogue filter where the
/// query shape is built up step-by-step and we want to inspect the generated
/// SQL before running it.
/// </summary>
/// <remarks>
/// The expression tree is translated to ClickHouse SQL by
/// <c>ClickHouseQueryProvider</c>. Use <c>.ToSql()</c> for free SQL inspection
/// during development, then <c>.AsAsyncEnumerable()</c> / <c>.ToListAsync()</c>
/// to execute. Where / Select / OrderBy[Descending] / ThenBy / Take / Skip are
/// all translated; aggregates and ClickHouse-specific operators
/// (Final / Sample / WithQueryId) live in their own samples.
/// </remarks>
internal static class LinqBasicsSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_catalogue_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    id       UInt32,
                    name     String,
                    category LowCardinality(String),
                    price    Float64,
                    rating   Float32,
                    quantity UInt32
                ) ENGINE = MergeTree()
                ORDER BY id
                """);

            await connection.ExecuteNonQueryAsync($"""
                INSERT INTO {tableName} VALUES
                    (1,  'Laptop',        'Electronics', 1299.99, 4.5,  50),
                    (2,  'Keyboard',      'Electronics',   79.99, 4.2, 200),
                    (3,  'Coffee Maker',  'Kitchen',      149.99, 4.7,  75),
                    (4,  'Desk Chair',    'Furniture',    399.99, 4.1,  30),
                    (5,  'Monitor',       'Electronics',  549.99, 4.6,  80),
                    (6,  'Blender',       'Kitchen',       69.99, 3.9, 120),
                    (7,  'Bookshelf',     'Furniture',    189.99, 4.3,  45),
                    (8,  'Mouse',         'Electronics',   49.99, 4.4, 300),
                    (9,  'Toaster',       'Kitchen',       34.99, 4.0, 150),
                    (10, 'Standing Desk', 'Furniture',    599.99, 4.8,  25)
                """);
            Console.WriteLine($"Seeded {tableName} with 10 catalogue items");

            using var cts = new CancellationTokenSource();

            // Where + OrderByDescending + Take — compositional query, materialised
            // when the IAsyncEnumerable is consumed. The closure-captured locals
            // (filterCategory, takeCount) act as parameters — they're translated
            // into the SQL at run-time, not interpolated as literals at write-time.
            var filterCategory = "Electronics";
            var takeCount = 3;
            var queryId = $"linq-top-{Guid.NewGuid():N}";

            var top3Electronics = connection.Table<CatalogueItem>(tableName)
                .Where(p => p.Category == filterCategory)
                .OrderByDescending(p => p.Price)
                .Take(takeCount)
                .WithQueryId(queryId);

            Console.WriteLine();
            Console.WriteLine("--- Top 3 Electronics by price ---");
            Console.WriteLine($"SQL: {top3Electronics.ToSql()}");
            await foreach (var p in top3Electronics.AsAsyncEnumerable().WithCancellation(cts.Token))
            {
                Console.WriteLine($"  {p.Name,-15} ${p.Price,8:F2}  rating={p.Rating}");
            }
            var topQueryIdEcho = connection.LastQueryId;

            // Select projection — ProductSummary carries only the columns the
            // caller actually needs, and the SELECT list narrows accordingly.
            // The async terminal accepts a CancellationToken too.
            var summary = connection.Table<CatalogueItem>(tableName)
                .Where(p => p.Category == "Furniture")
                .OrderBy(p => p.Price)
                .Select(p => new ProductSummary { Name = p.Name, Price = p.Price });

            Console.WriteLine();
            Console.WriteLine("--- Furniture summary (Select projection, ascending price) ---");
            Console.WriteLine($"SQL: {summary.ToSql()}");
            var summaryList = await summary.ToListAsync(cts.Token);
            foreach (var s in summaryList)
            {
                Console.WriteLine($"  {s.Name,-15} ${s.Price,8:F2}");
            }
            Console.WriteLine($"  ({summaryList.Count} row(s) materialised via ToListAsync)");

            // Skip + Take for paging — closure-captured locals double as
            // request parameters from the caller's perspective.
            var pageSize = 3;
            var pageOffset = 5;
            var page2 = connection.Table<CatalogueItem>(tableName)
                .OrderBy(p => p.Id)
                .Skip(pageOffset)
                .Take(pageSize);

            Console.WriteLine();
            Console.WriteLine("--- Page 2 (rows 6-8 by id) ---");
            Console.WriteLine($"SQL: {page2.ToSql()}");
            await foreach (var p in page2.AsAsyncEnumerable().WithCancellation(cts.Token))
            {
                Console.WriteLine($"  [{p.Id}] {p.Name}");
            }

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  Closure params     : filterCategory={filterCategory}, takeCount={takeCount}, pageSize={pageSize}, pageOffset={pageOffset}");
            Console.WriteLine($"  queryId via WithQueryId : {queryId}");
            Console.WriteLine($"  queryId echoed     : {topQueryIdEcho}");
            Console.WriteLine($"  Cancellation token : threaded into ToListAsync + WithCancellation on AsAsyncEnumerable");
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

file class CatalogueItem
{
    [ClickHouseColumn(Name = "id")] public uint Id { get; set; }
    [ClickHouseColumn(Name = "name")] public string Name { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "category")] public string Category { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "price")] public double Price { get; set; }
    [ClickHouseColumn(Name = "rating")] public float Rating { get; set; }
    [ClickHouseColumn(Name = "quantity")] public uint Quantity { get; set; }
}

file class ProductSummary
{
    [ClickHouseColumn(Name = "name")] public string Name { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "price")] public double Price { get; set; }
}
