using CH.Native.Samples.Queries;

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
    "scalar"          => ScalarSample.RunAsync,
    "reader"          => DataReaderSample.RunAsync,
    "rows"            => RawRowsSample.RunAsync,
    "typed"           => TypedSample.RunAsync,
    "typed-fast"      => TypedFastSample.RunAsync,
    "parameterized"   => ParameterizedSample.RunAsync,
    "linq"            => LinqBasicsSample.RunAsync,
    "linq-aggregates" => LinqAggregatesSample.RunAsync,
    "linq-final"      => LinqFinalSample.RunAsync,
    "linq-sample"     => LinqSampleClauseSample.RunAsync,
    "adonet"          => AdoNetSample.RunAsync,
    "dapper"          => DapperSample.RunAsync,
    "pooled"          => DataSourcePooledSample.RunAsync,
    "resilient"       => ResilientSample.RunAsync,
    "progress"        => ProgressCancellationSample.RunAsync,
    "log-analytics"   => LogAnalyticsSample.RunAsync,
    _                 => null,
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
        CH.Native query samples — pick one by name.

        Usage:
            dotnet run -- <sample> [connection-string]
            (default connection string: Host=localhost;Port=9000)

        Samples:
            scalar           connection.ExecuteScalarAsync<T>(sql, ...)
            reader           connection.ExecuteReaderAsync(sql, ...) — ClickHouseDataReader
            rows             connection.QueryAsync(sql) — IAsyncEnumerable<ClickHouseRow>
            typed            connection.QueryAsync<T>(sql) — reflection-mapped POCOs
            typed-fast       connection.QueryTypedAsync<T>(sql) — high-perf, no boxing
            parameterized    QueryAsync<T>(sql, params) — anon-obj / IDictionary parameters
            linq             connection.Table<T>(name).Where/Select/OrderBy/Take + ToSql
            linq-aggregates  CountAsync / SumAsync / AverageAsync / MinAsync / MaxAsync / Any / First / Single
            linq-final       connection.Table<T>(name).Final() — ReplacingMergeTree current state
            linq-sample      connection.Table<T>(name).Sample(0.1).WithQueryId(...) — approximate analytics
            adonet           ClickHouseDbConnection / ClickHouseDbCommand / DbDataReader
            dapper           Dapper QueryAsync / QueryFirstAsync / ExecuteScalarAsync after Register()
            pooled           dataSource.Table<T>(name) + concurrent rented-connection queries
            resilient        ResilientConnection — multi-host failover, retry policy
            progress         IProgress<QueryProgress> + CancellationToken — long-running query control
            log-analytics    Log dashboard — volume by level, latency by service, error rate, top slowest

        All samples create a uniquely-named temp table, run the demo, and drop the table.
        """);
}
