using CH.Native.Ado;
using CH.Native.Connection;
using CH.Native.Commands;
using CH.Native.Results;
using CH.Native.Connection;
using CH.Native.DependencyInjection;
using CH.Native.Dapper;
using Microsoft.Extensions.DependencyInjection;

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

        await using var connection = new ClickHouseConnection(connectionString);
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

            var products = await connection.QueryAsync<Product>(
                $"""
                SELECT id, name, category, price, in_stock
                FROM {tableName}
                WHERE category = @category AND price >= @minPrice
                ORDER BY price DESC
                """,
                new { category, minPrice },
                cts.Token);

            Console.WriteLine();
            Console.WriteLine($"--- {category} >= ${minPrice} (Dapper, anon-obj params) ---");
            foreach (var p in products)
            {
                Console.WriteLine($"  [{p.Id}] {p.Name,-12} ${p.Price,8:F2}  in_stock={p.InStock}");
            }

            // Array IN parameter — Dapper expands @ids into a list of bind
            // placeholders server-side, no manual list flattening needed.
            var pickIds = new uint[] { 1, 3, 5, 7 };
            var picks = await connection.QueryAsync<Product>(
                $"SELECT id, name, category, price, in_stock FROM {tableName} WHERE id IN @ids ORDER BY id",
                new { ids = pickIds },
                cts.Token);

            Console.WriteLine();
            Console.WriteLine($"--- Array IN @ids = [{string.Join(",", pickIds)}] ---");
            foreach (var p in picks)
            {
                Console.WriteLine($"  [{p.Id}] {p.Name}");
            }

            // QueryFirstAsync — single row.
            var cheapest = await connection.QueryFirstAsync<Product>(
                $"SELECT id, name, category, price, in_stock FROM {tableName} ORDER BY price ASC LIMIT 1",
                cancellationToken: cts.Token);
            Console.WriteLine($"\nCheapest product: {cheapest.Name} at ${cheapest.Price:F2}");

            // ExecuteScalarAsync — aggregate. IDbConnection extension; delegates to Dapper.
            var avgPrice = await connection.ExecuteScalarAsync<double>(
                $"SELECT round(avg(price), 2) FROM {tableName}");
            Console.WriteLine($"Average price  : ${avgPrice:F2}");

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  Anon-obj params       : @category, @minPrice bound");
            Console.WriteLine($"  Array IN parameter    : @ids = [{string.Join(",", pickIds)}]");
            Console.WriteLine($"  Mapping               : in_stock -> InStock (MatchNamesWithUnderscores)");
            Console.WriteLine($"  Cancellation token    : threaded via CH.Native.Dapper CancellationToken arg");
        }
        finally
        {
            await connection.ExecuteAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }

    /// <summary>
    /// Dapper-via-DI: shows the canonical pattern for apps that resolve
    /// <see cref="ClickHouseDataSource"/> through a service container and want
    /// to use Dapper extension methods on the pooled rent. Same surface as
    /// <see cref="RunAsync(string)"/>, just with the connection sourced from the
    /// pool — meaning credential providers, keyed services, pool stats, and
    /// resilience all keep working alongside Dapper.
    /// </summary>
    /// <remarks>
    /// The crucial line is <c>await ds.OpenConnectionAsync()</c>: it returns a
    /// <see cref="ClickHouseConnection"/>, which IS-A <see cref="System.Data.Common.DbConnection"/>.
    /// Because the variable is statically typed as <see cref="ClickHouseConnection"/>,
    /// the Dapper-style row methods bind to
    /// <see cref="CH.Native.Dapper.ClickHouseConnectionDapperExtensions"/> — the
    /// typed-mapper fast path — rather than to Dapper's IDbConnection extension.
    /// No wrapper, no manual conversion, no <c>OpenDbConnectionAsync</c>
    /// indirection — the same physical connection serves both the native API
    /// (<see cref="ClickHouseConnection.ExecuteScalarAsync{T}(string, System.Threading.CancellationToken)"/>,
    /// <see cref="ClickHouseConnection.CreateBulkInserter{T}(string, CH.Native.BulkInsert.BulkInsertOptions?)"/>)
    /// and the Dapper-style query surface.
    /// </remarks>
    public static async Task RunWithDependencyInjectionAsync(string connectionString)
    {
        ClickHouseDapperIntegration.Register();

        var tableName = $"sample_dapper_di_products_{Guid.NewGuid():N}";

        // Build a minimal service provider — the exact same calls work in
        // ASP.NET Core / Generic Host startup. AddClickHouse returns a
        // builder that supports .WithJwtProvider<...>() / .WithPasswordProvider<...>()
        // etc. — see the Hosting sample for those.
        var services = new ServiceCollection();
        services.AddClickHouse(connectionString);
        await using var sp = services.BuildServiceProvider();

        var ds = sp.GetRequiredService<ClickHouseDataSource>();

        // Pool-rented connection, idiomatic await using disposal returns it to
        // the pool. The rent is a ClickHouseConnection which is also a DbConnection.
        await using var connection = await ds.OpenConnectionAsync();

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
                    (4, 'Headphones', 'Electronics',  199.99, 1)
                """);
            Console.WriteLine($"Seeded {tableName} via DI-resolved DataSource");

            // The DataSource gave us a pooled ClickHouseConnection. The variable
            // is statically typed as ClickHouseConnection, so QueryAsync<T> binds
            // to ClickHouseConnectionDapperExtensions — CH.Native.Dapper's
            // typed-mapper fast path — rather than falling through to Dapper's
            // IDbConnection extension. No source change needed to opt in; the
            // receiver type does the work.
            var electronics = await connection.QueryAsync<Product>(
                $"SELECT id, name, category, price, in_stock FROM {tableName} WHERE category = @c ORDER BY price",
                new { c = "Electronics" });

            Console.WriteLine();
            Console.WriteLine("--- CH.Native.Dapper QueryAsync<T> on DI-rented connection ---");
            foreach (var p in electronics)
            {
                Console.WriteLine($"  [{p.Id}] {p.Name,-12} ${p.Price,8:F2}  in_stock={p.InStock}");
            }

            // Native API on the SAME pooled connection — proves the two surfaces
            // share the physical socket. Useful when you want Dapper for ad-hoc
            // queries and the native API for bulk insert / strongly-typed reads.
            var nativeAvg = await connection.ExecuteScalarAsync<double>(
                $"SELECT round(avg(price), 2) FROM {tableName}");
            Console.WriteLine($"\nNative ExecuteScalarAsync<double> avg: ${nativeAvg:F2}");

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  Resolved type         : {ds.GetType().Name}");
            Console.WriteLine($"  Rent type             : {connection.GetType().Name}");
            Console.WriteLine($"  Row mapper            : CH.Native.Dapper fast path (typed mapper, no boxing tax)");
            Console.WriteLine($"  Is DbConnection       : {connection is System.Data.Common.DbConnection}");
            Console.WriteLine($"  Pool stats after rent : {ds.GetStatistics()}");
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
