using System.Data;
using CH.Native.BulkInsert;
using CH.Native.Connection;

namespace CH.Native.Samples.Insert;

/// <summary>
/// <c>DynamicBulkInserter</c> — POCO-less bulk insert. Demonstrates three flavors:
/// (1) one-shot, (2) granular Init/Add/Complete, (3) pre-supplied <c>ColumnTypes</c>
/// to skip the server schema-probe round-trip.
/// </summary>
/// <remarks>
/// When the row shape isn't a static type — ETL pipelines reading from
/// <c>IDataReader</c>, dynamic schemas, dictionaries-of-values — the dynamic
/// inserter accepts rows as <c>object?[]</c> aligned to a caller-supplied
/// <c>columnNames</c> list. Same lifecycle as <c>BulkInserter&lt;T&gt;</c>;
/// always boxes (no direct-to-buffer fast path), so prefer the typed inserter
/// when the row type is known.
/// </remarks>
internal static class DynamicBulkInsertSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_dynamic_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    ts          DateTime,
                    event_type  LowCardinality(String),
                    user_id     UInt64,
                    properties  Map(String, String),
                    tags        Array(String)
                ) ENGINE = MergeTree()
                ORDER BY (event_type, ts)
                """);
            Console.WriteLine($"Created table {tableName}");

            var columns = new[] { "ts", "event_type", "user_id", "properties", "tags" };
            var t0 = new DateTime(2026, 5, 7, 9, 0, 0, DateTimeKind.Utc);

            // -----------------------------------------------------------------
            // (1) One-shot — easiest API. Pass column names + IEnumerable<object?[]>.
            // No POCO type, no [ClickHouseColumn] attributes.
            // -----------------------------------------------------------------
            await connection.BulkInsertAsync(
                tableName,
                columns,
                new[]
                {
                    new object?[]
                    {
                        t0, "click", 42UL,
                        new Dictionary<string, string> { ["page"] = "/home", ["region"] = "eu" },
                        new[] { "anon", "ab_test_a" }
                    },
                    new object?[]
                    {
                        t0.AddSeconds(5), "view", 43UL,
                        new Dictionary<string, string> { ["page"] = "/about" },
                        Array.Empty<string>()
                    },
                });
            Console.WriteLine("(1) One-shot: 2 rows");

            // -----------------------------------------------------------------
            // (2) Granular lifecycle — when you want to stream and react to
            // per-row errors before the implicit auto-flush kicks in.
            // -----------------------------------------------------------------
            await using (var inserter = connection.CreateBulkInserter(
                tableName, columns, new BulkInsertOptions { BatchSize = 1_000 }))
            {
                await inserter.InitAsync();

                var rng = new Random(7);
                var eventTypes = new[] { "click", "view", "purchase", "scroll" };
                for (var i = 0; i < 5_000; i++)
                {
                    await inserter.AddAsync(new object?[]
                    {
                        t0.AddSeconds(i),
                        eventTypes[i % eventTypes.Length],
                        (ulong)(100 + i),
                        new Dictionary<string, string> { ["seq"] = i.ToString() },
                        new[] { $"batch-{i / 1000}" }
                    });
                }

                await inserter.CompleteAsync();
            }
            Console.WriteLine("(2) Streaming insert: 5,000 rows");

            // -----------------------------------------------------------------
            // (3) Skip the server schema-probe by pre-supplying ColumnTypes.
            // Useful in steady-state pipelines where the schema is known and you
            // want to remove one round-trip per inserter init.
            // -----------------------------------------------------------------
            var optionsWithTypes = new BulkInsertOptions
            {
                ColumnTypes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["ts"]         = "DateTime",
                    ["event_type"] = "LowCardinality(String)",
                    ["user_id"]    = "UInt64",
                    ["properties"] = "Map(String, String)",
                    ["tags"]       = "Array(String)",
                },
                UseSchemaCache = false, // disable the cache so we can prove the round-trip is gone, not cached.
            };

            await connection.BulkInsertAsync(
                tableName, columns, FromDataTable(BuildDataTable(rowCount: 100, t0)), optionsWithTypes);
            Console.WriteLine("(3) Pre-typed insert (no schema probe): 100 rows");

            var total = await connection.ExecuteScalarAsync<ulong>($"SELECT count() FROM {tableName}");
            Console.WriteLine($"\nTotal rows: {total:N0}");

            Console.WriteLine("\n--- Counts by event_type ---");
            await foreach (var row in connection.QueryAsync(
                $"SELECT event_type, count() AS n FROM {tableName} GROUP BY event_type ORDER BY n DESC"))
            {
                Console.WriteLine($"  {row["event_type"]}: {row["n"]:N0}");
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }

    // IDataReader/DataTable adapter — shows how the dynamic API plugs into ETL
    // pipelines whose source is column-major and unknown at compile time.
    private static IEnumerable<object?[]> FromDataTable(DataTable dt)
    {
        var colCount = dt.Columns.Count;
        foreach (DataRow dr in dt.Rows)
        {
            var values = new object?[colCount];
            for (var c = 0; c < colCount; c++)
            {
                var v = dr[c];
                values[c] = v is DBNull ? null : v;
            }
            yield return values;
        }
    }

    private static DataTable BuildDataTable(int rowCount, DateTime t0)
    {
        var dt = new DataTable();
        dt.Columns.Add("ts", typeof(DateTime));
        dt.Columns.Add("event_type", typeof(string));
        dt.Columns.Add("user_id", typeof(ulong));
        dt.Columns.Add("properties", typeof(Dictionary<string, string>));
        dt.Columns.Add("tags", typeof(string[]));
        for (var i = 0; i < rowCount; i++)
        {
            dt.Rows.Add(
                t0.AddMinutes(i),
                i % 2 == 0 ? "click" : "view",
                (ulong)(10_000 + i),
                new Dictionary<string, string> { ["src"] = "etl" },
                new[] { "etl" });
        }
        return dt;
    }
}
