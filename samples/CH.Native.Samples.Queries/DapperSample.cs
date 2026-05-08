using CH.Native.Ado;
using CH.Native.Dapper;
using Dapper;

namespace CH.Native.Samples.Queries;

/// <summary>
/// Dapper integration via <c>ClickHouseDapperIntegration.Register()</c>. Models an
/// existing Dapper-flavoured codebase that wants to point at ClickHouse without
/// rewriting query code — <c>connection.QueryAsync&lt;T&gt;</c>,
/// <c>QueryFirstAsync</c>, <c>ExecuteScalarAsync</c>, anonymous-object params,
/// array <c>IN</c>, and snake_case → PascalCase mapping all just work.
/// </summary>
/// <remarks>
/// <c>Register()</c> wires up CH.Native's Dapper integration once at startup. It
/// installs type handlers for ClickHouse-native types (arrays, maps, etc.) and
/// flips Dapper's <c>MatchNamesWithUnderscores</c> so <c>in_stock</c> maps to
/// <c>InStock</c> without per-property attributes. Cancellation flows through
/// Dapper's <c>CommandDefinition</c>.
/// </remarks>
internal static class DapperSample
{
    public static async Task RunAsync(string connectionString)
    {
        // One-time integration setup. Idempotent — safe to call repeatedly.
        ClickHouseDapperIntegration.Register();

        var tableName = $"sample_dapper_products_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseDbConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteAsync($"""
                CREATE TABLE {tableName} (
                    id        UInt32,
                    name      String,
                    category  LowCardinality(String),
                    price     Float64,
                    in_stock  UInt8
                ) ENGINE = MergeTree()
                ORDER BY id
                """);

            await connection.ExecuteAsync($"""
                INSERT INTO {tableName} VALUES
                    (1, 'Laptop',     'Electronics', 1299.99, 1),
                    (2, 'Keyboard',   'Electronics',   79.99, 1),
                    (3, 'Coffee Mug', 'Kitchen',       12.99, 1),
                    (4, 'Desk Lamp',  'Office',        45.50, 0),
                    (5, 'Headphones', 'Electronics',  199.99, 1),
                    (6, 'Notebook',   'Office',         5.99, 1),
                    (7, 'Monitor',    'Electronics',  549.99, 1),
                    (8, 'Mug',        'Kitchen',       14.99, 0)
                """);
            Console.WriteLine($"Seeded {tableName} with 8 products");

            using var cts = new CancellationTokenSource();

            // Anonymous-object parameters — the canonical Dapper shape. Note
            // snake_case in_stock → PascalCase InStock thanks to the integration.
            var minPrice = 50.0;
            var category = "Electronics";

            var query = new CommandDefinition(
                $"""
                SELECT id, name, category, price, in_stock
                FROM {tableName}
                WHERE category = @category AND price >= @minPrice
                ORDER BY price DESC
                """,
                new { category, minPrice },
                cancellationToken: cts.Token);

            var products = await connection.QueryAsync<Product>(query);

            Console.WriteLine();
            Console.WriteLine($"--- {category} >= ${minPrice} (Dapper, anon-obj params) ---");
            foreach (var p in products)
            {
                Console.WriteLine($"  [{p.Id}] {p.Name,-12} ${p.Price,8:F2}  in_stock={p.InStock}");
            }

            // Array IN parameter — Dapper expands @ids into a list of bind
            // placeholders server-side, no manual list flattening needed.
            var pickIds = new uint[] { 1, 3, 5, 7 };
            var picksQuery = new CommandDefinition(
                $"SELECT id, name, category, price, in_stock FROM {tableName} WHERE id IN @ids ORDER BY id",
                new { ids = pickIds },
                cancellationToken: cts.Token);
            var picks = await connection.QueryAsync<Product>(picksQuery);

            Console.WriteLine();
            Console.WriteLine($"--- Array IN @ids = [{string.Join(",", pickIds)}] ---");
            foreach (var p in picks)
            {
                Console.WriteLine($"  [{p.Id}] {p.Name}");
            }

            // QueryFirstAsync — single row.
            var cheapest = await connection.QueryFirstAsync<Product>(new CommandDefinition(
                $"SELECT id, name, category, price, in_stock FROM {tableName} ORDER BY price ASC LIMIT 1",
                cancellationToken: cts.Token));
            Console.WriteLine($"\nCheapest product: {cheapest.Name} at ${cheapest.Price:F2}");

            // ExecuteScalarAsync — aggregate.
            var avgPrice = await connection.ExecuteScalarAsync<double>(new CommandDefinition(
                $"SELECT round(avg(price), 2) FROM {tableName}",
                cancellationToken: cts.Token));
            Console.WriteLine($"Average price  : ${avgPrice:F2}");

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  Anon-obj params       : @category, @minPrice bound");
            Console.WriteLine($"  Array IN parameter    : @ids = [{string.Join(",", pickIds)}]");
            Console.WriteLine($"  Mapping               : in_stock -> InStock (MatchNamesWithUnderscores)");
            Console.WriteLine($"  Cancellation token    : threaded via Dapper CommandDefinition");
        }
        finally
        {
            await connection.ExecuteAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

// Dapper maps by column name with MatchNamesWithUnderscores=true (set by
// ClickHouseDapperIntegration.Register), so in_stock → InStock without attributes.
file class Product
{
    public uint Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public double Price { get; set; }
    public byte InStock { get; set; }
}
