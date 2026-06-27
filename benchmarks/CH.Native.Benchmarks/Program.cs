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
        case "bulkalloc":
            BenchmarkRunner.Run<BulkInsertDirectVsBoxedAllocationBenchmarks>();
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
        case "schemacache":
            BenchmarkRunner.Run<BulkInsertSchemaCacheBenchmarks>();
            break;
        // Parallel (multi-connection) bulk insert: single vs Nx fan-out.
        // Extra args are forwarded to BenchmarkSwitcher (e.g. --iterationCount).
        case "parallelinsert":
            if (args.Length > 1)
            {
                BenchmarkSwitcher
                    .FromTypes(new[] { typeof(ParallelBulkInsertBenchmarks) })
                    .Run(args[1..]);
            }
            else
            {
                BenchmarkRunner.Run<ParallelBulkInsertBenchmarks>();
            }
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
        case "types":
            BenchmarkRunner.Run<BenchmarkTypeSuite>(new QuickComparisonConfig());
            break;
        case "all":
            RunAllBenchmarks();
            break;

        // Map benchmarks (no Docker required — unit-level)
        case "map":
            BenchmarkRunner.Run<MapColumnBenchmarks>();
            break;
        case "mapinspector":
            BenchmarkRunner.Run<MapShapeInspectorBenchmarks>();
            break;

        // Geo benchmarks (no Docker required — unit-level)
        case "geo":
            BenchmarkRunner.Run<GeoColumnBenchmarks>();
            break;
        case "multidim":
            BenchmarkRunner.Run<MultiDimArrayBenchmarks>();
            break;
        case "geocompare":
            BenchmarkRunner.Run<GeoComparisonBenchmarks>();
            break;

        // Variant benchmarks (micro: no Docker; compare: requires Docker)
        case "variant":
            BenchmarkRunner.Run<VariantColumnBenchmarks>();
            break;
        case "variantcompare":
            BenchmarkRunner.Run<VariantComparisonBenchmarks>();
            break;
        case "opt":
            BenchmarkRunner.Run<OptimizationBenchmarks>();
            break;

        // Pool / DataSource throughput (requires Docker)
        case "pool":
            BenchmarkRunner.Run<DataSourcePoolBenchmarks>();
            break;

        // Dapper vs native streaming comparison (requires Docker).
        // Extra args after "dapper" are forwarded to BenchmarkSwitcher so callers
        // can pass `--filter "*pattern*"` to narrow the run.
        case "dapper":
            if (args.Length > 1)
            {
                BenchmarkSwitcher
                    .FromTypes(new[] { typeof(DapperVsQueryStreamBenchmarks) })
                    .Run(args[1..]);
            }
            else
            {
                BenchmarkRunner.Run<DapperVsQueryStreamBenchmarks>();
            }
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
    BenchmarkRunner.Run<BulkInsertDirectVsBoxedAllocationBenchmarks>();
    BenchmarkRunner.Run<MapColumnBenchmarks>();
    BenchmarkRunner.Run<MapShapeInspectorBenchmarks>();

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
          bulkalloc   - Direct vs boxed bulk-insert allocation comparison (50K+ rows)
          geo         - Geo column reader/writer/skipper benchmarks
          map         - Map column reader/writer benchmarks (Dictionary vs ClickHouseMap)
          mapinspector- MapShapeInspector cache lookup benchmarks
          multidim    - Multi-dim array converters + Array(Array(Int32)) pipeline

        Protocol Comparison Benchmarks (requires Docker):
          geocompare  - Geo types: CH.Native (native TCP) vs ClickHouse.Driver (HTTP)

        Protocol Comparison Benchmarks (requires Docker):
          simple      - Simple query latency (SELECT 1, COUNT, 100 rows)
          large       - Large result set reads (10K, 100K, 1M rows)
          insert      - Bulk insert comparison (1K, 10K, 100K rows)
          parallelinsert - Parallel multi-connection bulk insert (1M, 10M rows; single vs Nx)
          schemacache - Per-connection schema cache (warm vs cold inserter)
          complex     - Complex queries (aggregations, JOINs, filters)
          connection  - Connection establishment overhead
          compression - Compression comparison (LZ4, Zstd, native vs HTTP)
          types       - Per-type read suite: Native vs Driver × Bulk/Single for every primitive
          pool        - DataSource pool throughput across MaxPoolSize × Parallelism matrix
          dapper      - Dapper QueryAsync vs native QueryStreamAsync (CH.Native vs ClickHouse.Driver)
          compare     - Run all protocol comparison benchmarks
          quick       - Quick dev test (SimpleQueryBenchmarks, few iterations)

        JSON Benchmarks (requires Docker with ClickHouse 25.6+):
          jsoncolumn  - JSON column reader/writer/skipper (unit-level, no Docker)
          jsonquery   - JSON query benchmarks (Native vs Driver vs Octonica)
          jsoninsert  - JSON bulk insert benchmarks (Native vs Driver vs Octonica)
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
