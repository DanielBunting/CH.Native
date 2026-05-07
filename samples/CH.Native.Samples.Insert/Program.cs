using CH.Native.Samples.Insert;

// Single-project samples runner. Pick a sample by name; the remaining argument is an
// optional connection string. Each sample creates a uniquely-named temp table, runs
// its demo against it, and drops the table on exit.

if (args.Length == 0 || args[0] is "-h" or "--help")
{
    PrintUsage();
    return;
}

var sampleName = args[0].ToLowerInvariant();
var connectionString = args.Length > 1 ? args[1] : "Host=localhost;Port=9000";

Func<string, Task>? runner = sampleName switch
{
    "single"      => SingleRecordSample.RunAsync,
    "collection"  => CollectionSample.RunAsync,
    "async"       => AsyncStreamSample.RunAsync,
    "oneshot"     => OneShotBulkInsertSample.RunAsync,
    "long-lived"  => LongLivedBulkInserterSample.RunAsync,
    "dynamic"     => DynamicBulkInsertSample.RunAsync,
    "pooled"      => DataSourcePooledSample.RunAsync,
    "cross-db"    => CrossDatabaseSample.RunAsync,
    "sql"         => PlainSqlInsertSample.RunAsync,
    _             => null,
};

if (runner is null)
{
    Console.Error.WriteLine($"Unknown sample '{sampleName}'.");
    PrintUsage();
    Environment.Exit(1);
}

Console.WriteLine($"--- Running '{sampleName}' against {connectionString} ---");
await runner(connectionString);

static void PrintUsage()
{
    Console.WriteLine("""
        CH.Native insert samples — pick one by name.

        Usage:
            dotnet run -- <sample> [connection-string]
            (default connection string: Host=localhost;Port=9000)

        Samples:
            single       connection.Table<T>(name).InsertAsync(row)
            collection   connection.Table<T>(name).InsertAsync(IEnumerable<T>)
            async        connection.Table<T>(name).InsertAsync(IAsyncEnumerable<T>)
            oneshot      connection.BulkInsertAsync<T>(name, rows, ...)
            long-lived   connection.CreateBulkInserter<T>(...) — Init/Add/Complete
            dynamic      DynamicBulkInserter — POCO-less, object?[] rows
            pooled       dataSource.Table<T>(name).InsertAsync(rows)
            cross-db     qualified database.table inserts on a single connection
            sql          plain SQL INSERT via ClickHouseCommand

        All samples create a uniquely-named temp table, run the demo, and drop the table.
        """);
}
