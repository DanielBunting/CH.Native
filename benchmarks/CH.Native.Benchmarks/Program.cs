using BenchmarkDotNet.Running;
using CH.Native.Benchmarks;
using CH.Native.Benchmarks.Benchmarks;
using CH.Native.Benchmarks.Infrastructure;

// Run specific benchmark based on args
if (args.Length > 0)
{
    switch (args[0].ToLowerInvariant())
    {
        // Existing synthetic benchmarks (no container required)
        case "bulk":
            BenchmarkRunner.Run<BulkInsertBenchmarks>();
            break;
        case "parser":
            BenchmarkRunner.Run<TypeParserBenchmarks>();
            break;

        // Protocol comparison benchmarks (requires Docker)
        case "simple":
            BenchmarkRunner.Run<SimpleQueryBenchmarks>();
            break;
        case "large":
            BenchmarkRunner.Run<LargeResultSetBenchmarks>();
            break;
        case "insert":
            BenchmarkRunner.Run<BulkInsertComparisonBenchmarks>();
            break;
        case "complex":
            BenchmarkRunner.Run<ComplexQueryBenchmarks>();
            break;
        case "connection":
            BenchmarkRunner.Run<ConnectionBenchmarks>();
            break;
        case "compression":
            BenchmarkRunner.Run<CompressionBenchmarks>();
            break;
        case "compare":
            RunComparisonBenchmarks();
            break;
        case "quick":
            // Quick run for development testing
            BenchmarkRunner.Run<SimpleQueryBenchmarks>(new QuickComparisonConfig());
            break;
        case "all":
            RunAllBenchmarks();
            break;

        // JSON benchmarks (requires Docker with ClickHouse 25.6+)
        case "jsoncolumn":
            BenchmarkRunner.Run<JsonColumnBenchmarks>();
            break;
        case "jsonquery":
            BenchmarkRunner.Run<JsonQueryBenchmarks>();
            break;
        case "jsoninsert":
            BenchmarkRunner.Run<JsonBulkInsertBenchmarks>();
            break;
        case "json":
            RunJsonBenchmarks();
            break;
        default:
            PrintUsage();
            break;
    }
}
else
{
    PrintUsage();
}

// Cleanup containers at the end
await BenchmarkContainerManager.Instance.DisposeAsync();
await JsonBenchmarkContainerManager.Instance.DisposeAsync();

static void RunComparisonBenchmarks()
{
    // Run all protocol comparison benchmarks
    BenchmarkRunner.Run<ConnectionBenchmarks>();
    BenchmarkRunner.Run<SimpleQueryBenchmarks>();
    BenchmarkRunner.Run<LargeResultSetBenchmarks>();
    BenchmarkRunner.Run<ComplexQueryBenchmarks>();
    BenchmarkRunner.Run<BulkInsertComparisonBenchmarks>();
    BenchmarkRunner.Run<CompressionBenchmarks>();
}

static void RunAllBenchmarks()
{
    // Synthetic benchmarks (no container)
    BenchmarkRunner.Run<TypeParserBenchmarks>();
    BenchmarkRunner.Run<BulkInsertBenchmarks>();

    // Protocol comparison benchmarks (requires Docker)
    RunComparisonBenchmarks();
}

static void RunJsonBenchmarks()
{
    // Unit-level JSON benchmarks (no container)
    BenchmarkRunner.Run<JsonColumnBenchmarks>();

    // Protocol comparison JSON benchmarks (requires Docker with CH 25.6+)
    BenchmarkRunner.Run<JsonQueryBenchmarks>();
    BenchmarkRunner.Run<JsonBulkInsertBenchmarks>();
}

static void PrintUsage()
{
    Console.WriteLine("""
        CH.Native Benchmarks

        Usage: dotnet run -c Release -- <benchmark>

        Synthetic Benchmarks (no Docker required):
          parser      - Type parser benchmarks
          bulk        - Bulk insert serialization benchmarks

        Protocol Comparison Benchmarks (requires Docker):
          simple      - Simple query latency (SELECT 1, COUNT, 100 rows)
          large       - Large result set reads (10K, 100K, 1M rows)
          insert      - Bulk insert comparison (1K, 10K, 100K rows)
          complex     - Complex queries (aggregations, JOINs, filters)
          connection  - Connection establishment overhead
          compression - Compression comparison (LZ4, Zstd, gzip)
          compare     - Run all protocol comparison benchmarks
          quick       - Quick dev test (SimpleQueryBenchmarks, few iterations)

        JSON Benchmarks (requires Docker with ClickHouse 25.6+):
          jsoncolumn  - JSON column reader/writer/skipper (unit-level, no Docker)
          jsonquery   - JSON query benchmarks (Native vs HTTP)
          jsoninsert  - JSON bulk insert benchmarks (Native vs HTTP)
          json        - Run all JSON benchmarks

        Combined:
          all         - Run all benchmarks (synthetic + comparison)

        Examples:
          dotnet run -c Release -- simple
          dotnet run -c Release -- compare
          dotnet run -c Release -- json
          dotnet run -c Release -- quick
        """);
}
