using System.Data;
using CH.Native.Ado;

namespace CH.Native.Samples.Queries;

/// <summary>
/// Standard ADO.NET surface — <c>ClickHouseDbConnection</c>,
/// <c>ClickHouseDbCommand</c>, <c>DbDataReader</c>, <c>DbParameter</c>. Models a
/// reporting tool that targets <c>System.Data.Common</c> abstractions and only
/// needs to know it's hitting a database, not which one.
/// </summary>
/// <remarks>
/// All reads go through the standard <c>DbCommand.ExecuteReaderAsync</c> /
/// <c>ExecuteScalarAsync</c> shapes — drop-in compatible with every
/// ADO.NET-flavoured framework. <c>cmd.Parameters.AddWithValue</c> binds
/// parameters; <c>cmd.CommandTimeout</c> sets a per-command timeout that
/// translates into per-row CancellationToken on the wire.
/// </remarks>
internal static class AdoNetSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_orders_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseDbConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            // DDL via the standard ExecuteNonQueryAsync. The cast is so we can
            // treat the ClickHouse-specific connection through the abstract
            // System.Data.Common surface.
            await using (var ddl = connection.CreateCommand())
            {
                ddl.CommandText = $"""
                    CREATE TABLE {tableName} (
                        order_id    UInt64,
                        customer    String,
                        total       Float64,
                        placed_at   DateTime
                    ) ENGINE = MergeTree()
                    ORDER BY order_id
                    """;
                await ddl.ExecuteNonQueryAsync();
            }

            await using (var seed = connection.CreateCommand())
            {
                seed.CommandText = $"""
                    INSERT INTO {tableName} VALUES
                        (1001, 'Alice',   125.50, '2026-05-01 10:00:00'),
                        (1002, 'Bob',     85.20,  '2026-05-01 11:15:00'),
                        (1003, 'Alice',   240.00, '2026-05-02 09:00:00'),
                        (1004, 'Carol',   55.75,  '2026-05-02 14:30:00'),
                        (1005, 'Bob',     310.99, '2026-05-03 08:45:00'),
                        (1006, 'Alice',   42.10,  '2026-05-03 16:00:00')
                    """;
                await seed.ExecuteNonQueryAsync();
            }
            Console.WriteLine($"Seeded {tableName} with 6 orders");

            using var cts = new CancellationTokenSource();

            // Parameterised SELECT via DbCommand + DbParameter. Bound by name
            // server-side, safe against injection. CommandTimeout becomes a
            // per-row deadline once the reader starts streaming.
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT order_id, customer, total, placed_at FROM {tableName} WHERE customer = @customer ORDER BY placed_at";
            cmd.CommandTimeout = 30; // seconds
            var customerParam = cmd.CreateParameter();
            customerParam.ParameterName = "customer";
            customerParam.Value = "Alice";
            cmd.Parameters.Add(customerParam);

            Console.WriteLine();
            Console.WriteLine($"--- Orders by Alice (DbCommand + DbParameter) ---");

            var rowCount = 0;
            decimal aliceTotal = 0;
            await using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cts.Token))
            {
                while (await reader.ReadAsync(cts.Token))
                {
                    // order_id is UInt64 — use the typed getter to avoid the
                    // potentially-lossy GetInt64 conversion.
                    var orderId = reader.GetFieldValue<ulong>(0);
                    var customer = reader.GetString(1);
                    var total = reader.GetDouble(2);
                    var placedAt = reader.GetDateTime(3);
                    aliceTotal += (decimal)total;
                    rowCount++;
                    Console.WriteLine($"  #{orderId} {customer} ${total:F2} at {placedAt:u}");
                }
            }

            // ExecuteScalarAsync via the abstract surface — boxed object return.
            await using var scalarCmd = connection.CreateCommand();
            scalarCmd.CommandText = $"SELECT count(DISTINCT customer) FROM {tableName}";
            var distinctCustomers = (ulong)(await scalarCmd.ExecuteScalarAsync(cts.Token))!;

            Console.WriteLine();
            Console.WriteLine($"Alice subtotal       : ${aliceTotal:F2} across {rowCount} order(s)");
            Console.WriteLine($"Distinct customers   : {distinctCustomers}");

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  DbParameter bound  : @customer = Alice");
            Console.WriteLine($"  CommandBehavior    : SequentialAccess");
            Console.WriteLine($"  CommandTimeout     : {cmd.CommandTimeout}s");
            Console.WriteLine($"  Cancellation token : threaded into ExecuteReaderAsync, ReadAsync, ExecuteScalarAsync");
        }
        finally
        {
            await using var teardown = connection.CreateCommand();
            teardown.CommandText = $"DROP TABLE IF EXISTS {tableName}";
            await teardown.ExecuteNonQueryAsync();
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}
