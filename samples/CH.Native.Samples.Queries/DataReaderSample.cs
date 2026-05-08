using CH.Native.Connection;

namespace CH.Native.Samples.Queries;

/// <summary>
/// <c>connection.ExecuteReaderAsync(sql)</c> — streaming through a
/// <c>ClickHouseDataReader</c>. Models a CSV export pipeline that reads a wide
/// table row-by-row and writes each row to an output stream without ever holding
/// the full result in memory.
/// </summary>
/// <remarks>
/// The reader is a forward-only cursor: <c>ReadAsync</c> advances to the next row
/// and <c>GetFieldValue&lt;T&gt;(ordinal)</c> pulls each column out by index. Use
/// <c>FieldCount</c> + <c>GetName</c> + <c>GetTypeName</c> for schema introspection
/// when the SELECT shape is dynamic. Memory stays bounded by the network buffer
/// regardless of result size.
/// </remarks>
internal static class DataReaderSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_export_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    user_id     UInt64,
                    email       String,
                    last_login  Nullable(DateTime),
                    score       Float64,
                    is_premium  UInt8
                ) ENGINE = MergeTree()
                ORDER BY user_id
                """);

            await connection.ExecuteNonQueryAsync($"""
                INSERT INTO {tableName} VALUES
                    (1001, 'alice@example.com',   '2026-05-06 09:14:00', 87.42, 1),
                    (1002, 'bob@example.com',     NULL,                  62.10, 0),
                    (1003, 'carol@example.com',   '2026-04-30 18:02:11', 95.05, 1),
                    (1004, 'dave@example.com',    '2026-05-07 07:00:00', 71.88, 0),
                    (1005, 'eve@example.com',     NULL,                  43.21, 0)
                """);
            Console.WriteLine($"Seeded {tableName} with 5 users");

            // Thread the full surface: custom queryId (correlates with system.query_log)
            // and CancellationToken (the reader honours it on every ReadAsync).
            using var cts = new CancellationTokenSource();
            var queryId = $"reader-export-{Guid.NewGuid():N}";

            await using var reader = await connection.ExecuteReaderAsync(
                $"SELECT user_id, email, last_login, score, is_premium FROM {tableName} ORDER BY user_id",
                cancellationToken: cts.Token,
                queryId: queryId);

            Console.WriteLine();
            Console.WriteLine("--- CSV export ---");

            // The reader needs ReadAsync() to fetch the first block before it
            // can answer schema questions like FieldCount / GetName / GetTypeName,
            // so we read first and then format the header from the live reader.
            var rowCount = 0;
            var headerEmitted = false;
            while (await reader.ReadAsync(cts.Token))
            {
                if (!headerEmitted)
                {
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        if (i > 0) Console.Write(',');
                        Console.Write($"{reader.GetName(i)}({reader.GetTypeName(i)})");
                    }
                    Console.WriteLine();
                    headerEmitted = true;
                }

                var userId = reader.GetFieldValue<ulong>(0);
                var email = reader.GetFieldValue<string>(1);
                var lastLogin = reader.IsDBNull(2)
                    ? "(never)"
                    : reader.GetFieldValue<DateTime>(2).ToString("u");
                var score = reader.GetFieldValue<double>(3);
                var premium = reader.GetFieldValue<byte>(4) == 1 ? "yes" : "no";

                Console.WriteLine($"{userId},{email},{lastLogin},{score:F2},{premium}");
                rowCount++;
            }

            Console.WriteLine($"\nExported {rowCount} row(s).");

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  queryId sent       : {queryId}");
            Console.WriteLine($"  queryId on reader  : {reader.QueryId}");
            Console.WriteLine($"  Cancellation token : threaded into ExecuteReaderAsync and ReadAsync");
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}
