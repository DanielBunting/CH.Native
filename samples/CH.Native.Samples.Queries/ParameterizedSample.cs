using CH.Native.Connection;
using CH.Native.Mapping;

namespace CH.Native.Samples.Queries;

/// <summary>
/// <c>connection.QueryAsync&lt;T&gt;(sql, parameters)</c> — server-bound parameters via
/// either an anonymous object or an <c>IDictionary&lt;string, object?&gt;</c>. Models
/// a search service composing a SELECT from user-supplied filter values without
/// ever string-concatenating into the SQL.
/// </summary>
/// <remarks>
/// Parameters use <c>@name</c> placeholders and are bound server-side by name —
/// safe against SQL injection, type-checked by the server. The same overloads
/// exist for <c>ExecuteScalarAsync&lt;T&gt;</c>, <c>ExecuteNonQueryAsync</c>, and the
/// raw <c>QueryAsync</c>. Use the anonymous-object form for static parameter sets;
/// use <c>Dictionary&lt;string, object?&gt;</c> when the parameter list is built dynamically.
/// </remarks>
internal static class ParameterizedSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_products_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    id        UInt32,
                    name      String,
                    category  LowCardinality(String),
                    price     Float64,
                    in_stock  UInt8
                ) ENGINE = MergeTree()
                ORDER BY id
                """);

            await connection.ExecuteNonQueryAsync($"""
                INSERT INTO {tableName} VALUES
                    (1,  'Laptop',         'Electronics', 1299.99, 1),
                    (2,  'Keyboard',       'Electronics',   79.99, 1),
                    (3,  'Coffee Maker',   'Kitchen',      149.99, 1),
                    (4,  'Desk Chair',     'Furniture',    399.99, 0),
                    (5,  'Monitor',        'Electronics',  549.99, 1),
                    (6,  'Blender',        'Kitchen',       69.99, 1),
                    (7,  'Bookshelf',      'Furniture',    189.99, 1),
                    (8,  'Mouse',          'Electronics',   49.99, 1),
                    (9,  'Toaster',        'Kitchen',       34.99, 0),
                    (10, 'Standing Desk',  'Furniture',    599.99, 1)
                """);
            Console.WriteLine($"Seeded {tableName} with 10 products");

            using var cts = new CancellationTokenSource();

            // (1) Anonymous-object parameters — the ergonomic default. Each property
            //     becomes a server-side named parameter (@category, @minPrice, @inStockOnly).
            //     CancellationToken is plumbed straight through.
            var category = "Electronics";
            var minPrice = 100.0;
            var inStockOnly = (byte)1;

            Console.WriteLine();
            Console.WriteLine($"--- Anonymous params: {category} > {minPrice:C} (in-stock only) ---");
            await foreach (var p in connection.QueryAsync<Product>(
                $"""
                SELECT id, name, category, price, in_stock
                FROM {tableName}
                WHERE category = @category
                  AND price > @minPrice
                  AND in_stock = @inStockOnly
                ORDER BY price DESC
                """,
                new { category, minPrice, inStockOnly },
                cts.Token))
            {
                Console.WriteLine($"  [{p.Id}] {p.Name,-15} {p.Price,9:F2}");
            }

            // (2) Dictionary-bound parameters — the same SQL, the same binding,
            //     but the parameter list is assembled at runtime. Useful when the
            //     filter set is dynamic (e.g. driven by a search-query parser).
            var dynamicFilters = new Dictionary<string, object?>
            {
                ["category"] = "Furniture",
                ["minPrice"] = 200.0,
                ["inStockOnly"] = (byte)1,
            };

            Console.WriteLine();
            Console.WriteLine("--- Dictionary params: Furniture > $200 (in-stock only) ---");
            await foreach (var p in connection.QueryAsync<Product>(
                $"""
                SELECT id, name, category, price, in_stock
                FROM {tableName}
                WHERE category = @category
                  AND price > @minPrice
                  AND in_stock = @inStockOnly
                ORDER BY price DESC
                """,
                dynamicFilters,
                cts.Token))
            {
                Console.WriteLine($"  [{p.Id}] {p.Name,-15} {p.Price,9:F2}");
            }

            // (3) Same parameter shape against a scalar — count the matches.
            var totalElectronicsValue = await connection.ExecuteScalarAsync<double>(
                $"SELECT sum(price) FROM {tableName} WHERE category = @category",
                new { category = "Electronics" },
                cts.Token);
            Console.WriteLine($"\nTotal Electronics catalogue value: ${totalElectronicsValue:N2}");

            // (4) The parameterised extensions don't surface a queryId knob, so
            //     for a traceable variant fall back to the non-parameterised
            //     overload with the value already interpolated into the SQL —
            //     safe here because the values are not user-supplied.
            var queryId = $"parameterized-trace-{Guid.NewGuid():N}";
            var traced = await connection.ExecuteScalarAsync<ulong>(
                $"SELECT count() FROM {tableName} WHERE category = 'Kitchen'",
                cancellationToken: cts.Token,
                queryId: queryId);

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  Parameters bound   : anon-obj + IDictionary<string, object?> shapes");
            Console.WriteLine($"  Cancellation token : threaded via cts.Token");
            Console.WriteLine($"  queryId sent       : {queryId}");
            Console.WriteLine($"  queryId echoed     : {connection.LastQueryId}");
            Console.WriteLine($"  Traced count       : {traced} Kitchen rows");
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

file class Product
{
    [ClickHouseColumn(Name = "id")] public uint Id { get; set; }
    [ClickHouseColumn(Name = "name")] public string Name { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "category")] public string Category { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "price")] public double Price { get; set; }
    [ClickHouseColumn(Name = "in_stock")] public byte InStock { get; set; }
}
