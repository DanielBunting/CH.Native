using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using CH.Native.Benchmarks.Models;
using CH.Native.BulkInsert;
using CH.Native.Connection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Measures the per-connection schema cache. Simulates a workload that creates many
/// short-lived <see cref="BulkInserter{T}"/> instances on the same connection — the cache
/// lets each repeat inserter skip one server round-trip for the schema exchange.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
[MemoryDiagnoser]
public class BulkInsertSchemaCacheBenchmarks
{
    private string _connectionString = null!;
    private InsertRow[] _testData = null!;

    /// <summary>Number of inserters to create on the same connection.</summary>
    [Params(10, 50)]
    public int Inserters { get; set; }

    /// <summary>Rows per inserter. Smaller rows make the schema RTT dominate.</summary>
    [Params(100)]
    public int RowsPerInserter { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();
        await TestDataGenerator.EnsureTablesCreatedAsync();

        _connectionString = BenchmarkContainerManager.Instance.NativeConnectionString;

        _testData = Enumerable.Range(0, RowsPerInserter)
            .Select(i => new InsertRow
            {
                Id = i,
                Name = $"SchemaCacheItem_{i}",
                Value = i * 1.5
            })
            .ToArray();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        TestDataGenerator.TruncateInsertTableAsync().GetAwaiter().GetResult();
    }

    [Benchmark(Baseline = true, Description = "No schema cache (cold inserter every time)")]
    public async Task Cold_NoCache()
    {
        await using var connection = new ClickHouseConnection(_connectionString);
        await connection.OpenAsync();

        var options = new BulkInsertOptions { BatchSize = 10_000, UseSchemaCache = false };

        for (int i = 0; i < Inserters; i++)
        {
            await connection.BulkInsertAsync(TestDataGenerator.InsertTable, _testData, options);
        }
    }

    [Benchmark(Description = "With schema cache (warm after first inserter)")]
    public async Task Warm_WithCache()
    {
        await using var connection = new ClickHouseConnection(_connectionString);
        await connection.OpenAsync();

        var options = new BulkInsertOptions { BatchSize = 10_000, UseSchemaCache = true };

        for (int i = 0; i < Inserters; i++)
        {
            await connection.BulkInsertAsync(TestDataGenerator.InsertTable, _testData, options);
        }
    }
}
