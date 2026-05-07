using System.Data;
using CH.Native.BulkInsert;
using CH.Native.Connection;

// Demonstrates POCO-less bulk insert via DynamicBulkInserter — useful when the
// row shape isn't known at compile time. Common cases: ETL pipelines reading
// from IDataReader, dynamic schemas, dictionaries-of-values.
//
// Three flavors are shown:
//   1. One-shot BulkInsertAsync with IEnumerable<object?[]>.
//   2. Granular Init/Add/Flush/Complete lifecycle for streaming + per-row error handling.
//   3. Init with caller-supplied ColumnTypes — skips the schema-probe round-trip.

var connectionString = args.Length > 0 ? args[0] : "Host=localhost;Port=9000";
var tableName = $"sample_dynamic_{Guid.NewGuid():N}";

await using var connection = new ClickHouseConnection(connectionString);
await connection.OpenAsync();
Console.WriteLine("Connected to ClickHouse");

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

try
{
    // ---------------------------------------------------------------------
    // (1) One-shot — easiest API. Pass column names + IEnumerable<object?[]>.
    // No POCO type, no [ClickHouseColumn] attributes.
    // ---------------------------------------------------------------------
    var columns = new[] { "ts", "event_type", "user_id", "properties", "tags" };
    var t0 = new DateTime(2026, 5, 7, 9, 0, 0, DateTimeKind.Utc);

    await connection.BulkInsertAsync(
        tableName,
        columns,
        new[]
        {
            new object?[]
            {
                t0,
                "click",
                42UL,
                new Dictionary<string, string> { ["page"] = "/home", ["region"] = "eu" },
                new[] { "anon", "ab_test_a" }
            },
            new object?[]
            {
                t0.AddSeconds(5),
                "view",
                43UL,
                new Dictionary<string, string> { ["page"] = "/about" },
                Array.Empty<string>()
            },
        });
    Console.WriteLine("One-shot insert: 2 rows");

    // ---------------------------------------------------------------------
    // (2) Granular lifecycle — when you want to stream and react to per-row
    // errors before the implicit auto-flush kicks in. The shape is:
    //   await using inserter; await Init; await Add+; await Complete;
    // ---------------------------------------------------------------------
    await using (var inserter = connection.CreateBulkInserter(
        tableName,
        columns,
        new BulkInsertOptions { BatchSize = 1_000 }))
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
    Console.WriteLine("Streaming insert: 5,000 rows");

    // ---------------------------------------------------------------------
    // (3) Skip the server schema-probe entirely by pre-supplying ColumnTypes.
    // Useful in steady-state pipelines where the schema is known and you want
    // to remove one round-trip per inserter init.
    // ---------------------------------------------------------------------
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
        tableName,
        columns,
        FromDataTable(BuildDataTable(rowCount: 100, t0: t0)),
        optionsWithTypes);
    Console.WriteLine("Pre-typed insert (no schema probe): 100 rows");

    // ---------------------------------------------------------------------
    // Verify totals + a quick aggregation.
    // ---------------------------------------------------------------------
    var total = await connection.ExecuteScalarAsync<ulong>($"SELECT count() FROM {tableName}");
    Console.WriteLine($"\nTotal rows: {total:N0}");

    Console.WriteLine("\n--- Counts by event_type ---");
    await foreach (var row in connection.QueryAsync(
        $"SELECT event_type, count() AS n FROM {tableName} GROUP BY event_type ORDER BY n DESC"))
    {
        Console.WriteLine($"  {row["event_type"]}: {row["n"]}");
    }
}
finally
{
    await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
    Console.WriteLine($"\nDropped table {tableName}");
}

// IDataReader/DataTable adapter — shows how the dynamic API plugs into ETL
// pipelines whose source is column-major and unknown at compile time.
static IEnumerable<object?[]> FromDataTable(DataTable dt)
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

static DataTable BuildDataTable(int rowCount, DateTime t0)
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
