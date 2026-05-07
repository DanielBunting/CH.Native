using CH.Native.Commands;
using CH.Native.Connection;

namespace CH.Native.Samples.Insert;

/// <summary>
/// Plain SQL <c>INSERT</c> via <c>ClickHouseCommand</c>. Demonstrates three flavors:
/// (1) parameterised <c>INSERT ... VALUES</c>, (2) <c>INSERT ... SELECT</c> for
/// table-to-table copies, (3) bulk <c>INSERT INTO ... VALUES (...), (...)</c> with
/// inline literals.
/// </summary>
/// <remarks>
/// Non-bulk path. Each row goes as text in the query string rather than as a
/// binary column block, so it's slower than the bulk-insert paths and pays full
/// SQL parsing cost server-side. Acceptable for occasional admin operations or
/// cross-table copies; not for hot ingestion paths — reach for the bulk APIs there.
/// </remarks>
internal static class PlainSqlInsertSample
{
    public static async Task RunAsync(string connectionString)
    {
        var sourceTable = $"sample_sql_src_{Guid.NewGuid():N}";
        var destTable = $"sample_sql_dst_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {sourceTable} (
                    id   UInt32,
                    name String,
                    role LowCardinality(String)
                ) ENGINE = MergeTree() ORDER BY id
                """);
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {destTable} (
                    id   UInt32,
                    name String,
                    role LowCardinality(String)
                ) ENGINE = MergeTree() ORDER BY id
                """);
            Console.WriteLine($"Created source={sourceTable} and dest={destTable}");

            // -----------------------------------------------------------------
            // (1) Parameterised single-row INSERT. The {p:Type} placeholders are
            // server-side bound, so the text path stays SQL-injection-safe even
            // when row values contain quotes or other hostile characters.
            // -----------------------------------------------------------------
            await using (var cmd = new ClickHouseCommand(connection)
            {
                CommandText = $"INSERT INTO {sourceTable} (id, name, role) VALUES " +
                              "({p_id:UInt32}, {p_name:String}, {p_role:String})"
            })
            {
                cmd.Parameters.Add(new ClickHouseParameter("p_id", 1));
                cmd.Parameters.Add(new ClickHouseParameter("p_name", "Alice"));
                cmd.Parameters.Add(new ClickHouseParameter("p_role", "admin"));

                // ClickHouse's native protocol does not track per-statement row
                // deltas for INSERT ... VALUES, so ExecuteNonQueryAsync returns 0
                // even on a successful insert. The SELECT count() below confirms it.
                _ = await cmd.ExecuteNonQueryAsync();
            }
            Console.WriteLine("(1) Parameterised single-row INSERT: 1 row");

            // -----------------------------------------------------------------
            // (2) Inline-literal multi-row INSERT — the simplest form, fine for
            // small static seeds. Don't build this string from user input — use
            // parameters or the bulk API instead.
            // -----------------------------------------------------------------
            await connection.ExecuteNonQueryAsync($"""
                INSERT INTO {sourceTable} (id, name, role) VALUES
                    (2, 'Bob',     'editor'),
                    (3, 'Charlie', 'editor'),
                    (4, 'Diana',   'viewer')
                """);
            Console.WriteLine("(2) Inline VALUES INSERT: 3 rows");

            // -----------------------------------------------------------------
            // (3) INSERT ... SELECT — server-side copy from one table to another
            // without round-tripping the row data through the client. Useful for
            // backfills, materialisations, and admin ops.
            // -----------------------------------------------------------------
            await connection.ExecuteNonQueryAsync(
                $"INSERT INTO {destTable} SELECT * FROM {sourceTable} WHERE role != 'viewer'");
            Console.WriteLine("(3) INSERT ... SELECT (excluding viewers)");

            var srcCount = await connection.ExecuteScalarAsync<ulong>($"SELECT count() FROM {sourceTable}");
            var dstCount = await connection.ExecuteScalarAsync<ulong>($"SELECT count() FROM {destTable}");
            Console.WriteLine($"\nSource has {srcCount:N0} rows; dest has {dstCount:N0}.");

            Console.WriteLine("\n--- Source rows by role ---");
            await foreach (var row in connection.QueryAsync(
                $"SELECT role, count() AS n FROM {sourceTable} GROUP BY role ORDER BY role"))
            {
                Console.WriteLine($"  {row["role"]}: {row["n"]}");
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {sourceTable}");
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {destTable}");
            Console.WriteLine($"\nDropped both tables");
        }
    }
}
