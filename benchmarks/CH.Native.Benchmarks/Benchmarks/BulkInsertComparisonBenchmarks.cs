using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using CH.Native.Benchmarks.Models;
using CH.Native.BulkInsert;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using HttpConnection = ClickHouse.Client.ADO.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks for bulk insert operations (write performance).
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class BulkInsertComparisonBenchmarks
{
    private string _nativeConnectionString = null!;
    private string _httpConnectionString = null!;
    private InsertRow[] _testData = null!;

    [Params(1_000, 10_000, 100_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();
        await TestDataGenerator.EnsureTablesCreatedAsync();

        var manager = BenchmarkContainerManager.Instance;
        _nativeConnectionString = manager.NativeConnectionString;
        _httpConnectionString = manager.HttpConnectionString;

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

    // --- HTTP Bulk Insert (ClickHouseBulkCopy) ---

    [Benchmark(Description = "Bulk Insert - HTTP")]
    public async Task Http_BulkInsert()
    {
        using var connection = new HttpConnection(_httpConnectionString);
        await connection.OpenAsync();

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = TestDataGenerator.InsertTable,
            BatchSize = 10_000
        };

        // Convert to object[][] format required by ClickHouseBulkCopy
        var rows = _testData.Select(r => new object[] { r.Id, r.Name, r.Value });
        await bulkCopy.WriteToServerAsync(rows);
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

    // --- HTTP with streaming source ---

    [Benchmark(Description = "Bulk Insert Streaming - HTTP")]
    public async Task Http_BulkInsertStreaming()
    {
        using var connection = new HttpConnection(_httpConnectionString);
        await connection.OpenAsync();

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = TestDataGenerator.InsertTable,
            BatchSize = 10_000
        };

        await bulkCopy.WriteToServerAsync(GenerateRows());

        IEnumerable<object[]> GenerateRows()
        {
            for (int i = 0; i < RowCount; i++)
            {
                yield return new object[] { (long)i, $"StreamItem_{i}", i * 1.5 };
            }
        }
    }
}
