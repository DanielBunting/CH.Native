using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using CH.Native.Benchmarks.Models;
using CH.Native.BulkInsert;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using OctonicaConnection = Octonica.ClickHouseClient.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks for bulk insert operations (write performance).
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class BulkInsertComparisonBenchmarks
{
    private string _nativeConnectionString = null!;
    private string _driverConnectionString = null!;
    private string _octonicaConnectionString = null!;
    private InsertRow[] _testData = null!;

    [Params(10_000, 100_000, 1_000_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();
        await TestDataGenerator.EnsureTablesCreatedAsync();

        var manager = BenchmarkContainerManager.Instance;
        _nativeConnectionString = manager.NativeConnectionString;
        _driverConnectionString = manager.DriverConnectionString;
        _octonicaConnectionString = manager.OctonicaConnectionString;

        // Pre-generate test data
        _testData = Enumerable.Range(0, RowCount)
            .Select(i => new InsertRow
            {
                Id = i,
                Name = $"BulkItem_{i}",
                Value = i * 1.5
            })
            .ToArray();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        // Truncate table before each iteration
        TestDataGenerator.TruncateInsertTableAsync().GetAwaiter().GetResult();
    }

    // --- Native Bulk Insert ---

    [Benchmark(Description = "Bulk Insert - Native")]
    public async Task Native_BulkInsert()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();

        await connection.BulkInsertAsync(
            TestDataGenerator.InsertTable,
            _testData,
            new BulkInsertOptions { BatchSize = 10_000 });
    }

    // --- Driver Bulk Insert (ClickHouse.Driver InsertBinaryAsync) ---

    [Benchmark(Description = "Bulk Insert - Driver")]
    public async Task Driver_BulkInsert()
    {
        using var client = new ClickHouse.Driver.ClickHouseClient(_driverConnectionString);

        var columns = new[] { "id", "name", "value" };
        var rows = _testData.Select(r => new object[] { r.Id, r.Name, r.Value });
        await client.InsertBinaryAsync(TestDataGenerator.InsertTable, columns, rows);
    }

    // --- Octonica Bulk Insert (columnar API) ---

    [Benchmark(Description = "Bulk Insert - Octonica")]
    public async Task Octonica_BulkInsert()
    {
        await using var connection = new OctonicaConnection(_octonicaConnectionString);
        await connection.OpenAsync();

        var ids = _testData.Select(r => r.Id).ToList();
        var names = _testData.Select(r => r.Name).ToList();
        var values = _testData.Select(r => r.Value).ToList();

        await using var writer = await connection.CreateColumnWriterAsync(
            $"INSERT INTO {TestDataGenerator.InsertTable}(id, name, value) VALUES",
            CancellationToken.None);
        await writer.WriteTableAsync(
            new object[] { ids, names, values },
            _testData.Length,
            CancellationToken.None);
    }

    // --- Native with streaming source ---

    [Benchmark(Description = "Bulk Insert Streaming - Native")]
    public async Task Native_BulkInsertStreaming()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();

        await connection.BulkInsertAsync(TestDataGenerator.InsertTable, GenerateRows());

        IEnumerable<InsertRow> GenerateRows()
        {
            for (int i = 0; i < RowCount; i++)
            {
                yield return new InsertRow
                {
                    Id = i,
                    Name = $"StreamItem_{i}",
                    Value = i * 1.5
                };
            }
        }
    }

    // --- Driver with streaming source ---

    [Benchmark(Description = "Bulk Insert Streaming - Driver")]
    public async Task Driver_BulkInsertStreaming()
    {
        using var client = new ClickHouse.Driver.ClickHouseClient(_driverConnectionString);

        var columns = new[] { "id", "name", "value" };
        await client.InsertBinaryAsync(TestDataGenerator.InsertTable, columns, GenerateRows());

        IEnumerable<object[]> GenerateRows()
        {
            for (int i = 0; i < RowCount; i++)
            {
                yield return new object[] { (long)i, $"StreamItem_{i}", i * 1.5 };
            }
        }
    }
}
