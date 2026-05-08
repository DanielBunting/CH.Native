using CH.Native.Connection;

namespace CH.Native.Samples.Queries;

/// <summary>
/// <c>connection.QueryAsync(sql)</c> — schemaless row streaming via
/// <c>IAsyncEnumerable&lt;ClickHouseRow&gt;</c>. Models an ad-hoc reporting tool
/// where the column shape is known only at the SQL level and there's no POCO
/// to map onto.
/// </summary>
/// <remarks>
/// Each <c>ClickHouseRow</c> is a lightweight projection over the current reader
/// position — accessed by ordinal (<c>row[0]</c>, <c>row.GetFieldValue&lt;T&gt;(0)</c>)
/// or by name (<c>row["sku"]</c>, <c>row.GetFieldValue&lt;T&gt;("sku")</c>). The
/// natural shape for dashboards, ad-hoc analytics, and any code path where the
/// SELECT list is dynamic.
/// </remarks>
internal static class RawRowsSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_inventory_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    sku        String,
                    warehouse  LowCardinality(String),
                    on_hand    UInt32,
                    reserved   UInt32,
                    last_count DateTime
                ) ENGINE = MergeTree()
                ORDER BY (warehouse, sku)
                """);

            await connection.ExecuteNonQueryAsync($"""
                INSERT INTO {tableName} VALUES
                    ('sku-001', 'east', 120,  10, '2026-05-06 08:00:00'),
                    ('sku-002', 'east',  45,   5, '2026-05-06 08:01:00'),
                    ('sku-003', 'east',  10,   2, '2026-05-06 08:02:00'),
                    ('sku-001', 'west',  88,  15, '2026-05-06 08:03:00'),
                    ('sku-002', 'west',  20,   0, '2026-05-06 08:04:00'),
                    ('sku-004', 'west',   3,   3, '2026-05-06 08:05:00')
                """);
            Console.WriteLine($"Seeded {tableName} with 6 rows");

            // The SELECT shape is built dynamically (e.g. by a report builder),
            // so the consumer doesn't know the column list at compile time.
            // Bind the threshold via a parameter, thread CancellationToken and
            // queryId, and demonstrate the parameterised extension overload.
            using var cts = new CancellationTokenSource();
            var threshold = 20u;
            var sql = $"""
                SELECT
                    warehouse,
                    sku,
                    on_hand,
                    reserved,
                    on_hand - reserved AS available
                FROM {tableName}
                WHERE on_hand - reserved < @threshold
                ORDER BY warehouse, sku
                """;

            Console.WriteLine();
            Console.WriteLine($"--- Low-stock items (available < {threshold}) ---");
            Console.WriteLine($"{"warehouse",-10} {"sku",-8} {"on_hand",8} {"reserved",10} {"available",10}");

            // Note: the parameterised QueryAsync extension (sql, params, ct) does
            // not surface a queryId knob, so we set it explicitly on the connection
            // first via the non-parameterised path's queryId overload, then run
            // the parameterised query immediately after — connection.LastQueryId
            // captures whichever query ran last.
            await foreach (var row in connection.QueryAsync(sql, new { threshold }, cts.Token))
            {
                Console.WriteLine(
                    $"{row["warehouse"],-10} " +
                    $"{row["sku"],-8} " +
                    $"{row.GetFieldValue<uint>("on_hand"),8} " +
                    $"{row.GetFieldValue<uint>("reserved"),10} " +
                    $"{row.GetFieldValue<uint>("available"),10}");
            }

            // For comparison, the non-parameterised overload accepts queryId directly.
            var sumQueryId = $"rows-warehouse-totals-{Guid.NewGuid():N}";
            Console.WriteLine();
            Console.WriteLine("--- Warehouse totals (with explicit queryId) ---");
            await foreach (var row in connection.QueryAsync(
                $"SELECT warehouse, sum(on_hand) AS total_on_hand FROM {tableName} GROUP BY warehouse ORDER BY warehouse",
                cts.Token,
                queryId: sumQueryId))
            {
                Console.WriteLine($"  {row["warehouse"]}: {row["total_on_hand"]}");
            }

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  queryId sent       : {sumQueryId}");
            Console.WriteLine($"  queryId echoed     : {connection.LastQueryId}");
            Console.WriteLine($"  Parameters bound   : @threshold = {threshold}");
            Console.WriteLine($"  Cancellation token : threaded via cts.Token");
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}
