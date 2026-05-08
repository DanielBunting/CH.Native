using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;

namespace CH.Native.Samples.Queries;

/// <summary>
/// LINQ async aggregates — <c>CountAsync</c>, <c>LongCountAsync</c>, <c>SumAsync</c>,
/// <c>AverageAsync</c>, <c>MinAsync</c>, <c>MaxAsync</c>, <c>AnyAsync</c>,
/// <c>AllAsync</c>, <c>FirstAsync</c>, <c>FirstOrDefaultAsync</c>, <c>SingleAsync</c>,
/// <c>SingleOrDefaultAsync</c>. Models a dashboard fan-out where each tile is one
/// aggregate query.
/// </summary>
/// <remarks>
/// Each terminal operator translates to a single server-side aggregate (or
/// existence) query — there's no client-side reduction over a streamed set.
/// Combine with <c>.Where(...)</c> earlier in the chain to filter before the
/// aggregate runs.
/// </remarks>
internal static class LinqAggregatesSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_metrics_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    id       UInt32,
                    region   LowCardinality(String),
                    revenue  Float64,
                    orders   UInt32,
                    is_vip   UInt8
                ) ENGINE = MergeTree()
                ORDER BY id
                """);

            await connection.ExecuteNonQueryAsync($"""
                INSERT INTO {tableName} VALUES
                    (1, 'east',  1250.00, 42, 1),
                    (2, 'east',   880.50, 31, 0),
                    (3, 'west',  2300.75, 88, 1),
                    (4, 'west',   450.00, 15, 0),
                    (5, 'south', 1100.25, 50, 1),
                    (6, 'south',  720.00, 27, 0),
                    (7, 'north',  990.10, 35, 1),
                    (8, 'north', 1850.00, 70, 1)
                """);
            Console.WriteLine($"Seeded {tableName} with 8 metric rows");

            using var cts = new CancellationTokenSource();
            var table = connection.Table<Metric>(tableName);

            // Each call below is a separate round-trip. CancellationToken is
            // threaded through every terminal operator so any one of them can
            // be aborted by signalling the source. The closure-captured locals
            // (vipFilter, highRollerCutoff) act as parameters in the SQL.
            var vipFilter = (byte)1;
            var highRollerCutoff = 2000.0;

            var totalCount = await table.CountAsync(cts.Token);
            var longCount = await table.LongCountAsync(cts.Token);
            var totalRevenue = await table.SumAsync(m => m.Revenue, cts.Token);
            var avgRevenue = await table.AverageAsync(m => m.Revenue, cts.Token);
            var minOrders = await table.MinAsync(m => m.Orders, cts.Token);
            var maxOrders = await table.MaxAsync(m => m.Orders, cts.Token);
            var hasAnyEast = await table.Where(m => m.Region == "east").AnyAsync(cts.Token);
            var allHaveOrders = await table.AllAsync(m => m.Orders > 0, cts.Token);
            var firstByRevenue = await table.OrderByDescending(m => m.Revenue).FirstAsync(cts.Token);
            var firstOrDefaultMissing = await table.Where(m => m.Region == "polar").FirstOrDefaultAsync(cts.Token);
            var singleHighRoller = await table
                .Where(m => m.Revenue > highRollerCutoff)
                .SingleAsync(cts.Token);
            var singleOrDefaultMissing = await table
                .Where(m => m.Revenue > 99999)
                .SingleOrDefaultAsync(cts.Token);
            var vipCount = await table.Where(m => m.IsVip == vipFilter).CountAsync(cts.Token);

            Console.WriteLine();
            Console.WriteLine("--- Dashboard fan-out (one round-trip per tile) ---");
            Console.WriteLine($"  CountAsync                : {totalCount}");
            Console.WriteLine($"  LongCountAsync            : {longCount}");
            Console.WriteLine($"  SumAsync(Revenue)         : ${totalRevenue:N2}");
            Console.WriteLine($"  AverageAsync(Revenue)     : ${avgRevenue:N2}");
            Console.WriteLine($"  MinAsync(Orders)          : {minOrders}");
            Console.WriteLine($"  MaxAsync(Orders)          : {maxOrders}");
            Console.WriteLine($"  AnyAsync(region == east)  : {hasAnyEast}");
            Console.WriteLine($"  AllAsync(orders > 0)      : {allHaveOrders}");
            Console.WriteLine($"  FirstAsync(top revenue)   : id={firstByRevenue.Id} revenue=${firstByRevenue.Revenue:N2}");
            Console.WriteLine($"  FirstOrDefaultAsync(none) : {(firstOrDefaultMissing is null ? "(null)" : firstOrDefaultMissing.Region)}");
            Console.WriteLine($"  SingleAsync(>{2000:N0})       : id={singleHighRoller.Id} revenue=${singleHighRoller.Revenue:N2}");
            Console.WriteLine($"  SingleOrDefaultAsync(none): {(singleOrDefaultMissing is null ? "(null)" : singleOrDefaultMissing.Region)}");
            Console.WriteLine($"  CountAsync(VIPs only)     : {vipCount}");

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  Closure params     : vipFilter={vipFilter}, highRollerCutoff={highRollerCutoff:N0}");
            Console.WriteLine($"  Cancellation token : threaded into every terminal operator above");
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

file class Metric
{
    [ClickHouseColumn(Name = "id")] public uint Id { get; set; }
    [ClickHouseColumn(Name = "region")] public string Region { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "revenue")] public double Revenue { get; set; }
    [ClickHouseColumn(Name = "orders")] public uint Orders { get; set; }
    [ClickHouseColumn(Name = "is_vip")] public byte IsVip { get; set; }
}
