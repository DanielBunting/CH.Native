using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using CH.Native.Benchmarks.Models;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;
using OctonicaConnection = Octonica.ClickHouseClient.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks for reading large result sets.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class LargeResultSetBenchmarks
{
    private NativeConnection _nativeConnection = null!;
    private DriverConnection _driverConnection = null!;
    private OctonicaConnection _octonicaConnection = null!;

    [Params(10_000, 100_000, 1_000_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();
        await TestDataGenerator.EnsureTablesCreatedAsync();

        var manager = BenchmarkContainerManager.Instance;

        _nativeConnection = new NativeConnection(manager.NativeConnectionString);
        await _nativeConnection.OpenAsync();

        _driverConnection = new DriverConnection(manager.DriverConnectionString);
        await _driverConnection.OpenAsync();

        _octonicaConnection = new OctonicaConnection(manager.OctonicaConnectionString);
        await _octonicaConnection.OpenAsync();
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        await _nativeConnection.DisposeAsync();
        await _driverConnection.DisposeAsync();
        await _octonicaConnection.DisposeAsync();
    }

    // --- Streaming read (row-by-row) ---

    [Benchmark(Description = "Streaming Read - Native")]
    public async Task<long> Native_StreamingRead()
    {
        long sum = 0;
        await foreach (var row in _nativeConnection.QueryAsync(
            $"SELECT * FROM {TestDataGenerator.LargeTable} LIMIT {RowCount}"))
        {
            sum += row.GetFieldValue<long>("id");
        }
        return sum;
    }

    [Benchmark(Description = "Streaming Read - Driver")]
    public async Task<long> Driver_StreamingRead()
    {
        using var cmd = _driverConnection.CreateCommand();
        cmd.CommandText = $"SELECT * FROM {TestDataGenerator.LargeTable} LIMIT {RowCount}";
        using var reader = await cmd.ExecuteReaderAsync();

        long sum = 0;
        while (await reader.ReadAsync())
        {
            sum += reader.GetInt64(0); // id column
        }
        return sum;
    }

    [Benchmark(Description = "Streaming Read - Octonica")]
    public async Task<long> Octonica_StreamingRead()
    {
        using var cmd = _octonicaConnection.CreateCommand(
            $"SELECT * FROM {TestDataGenerator.LargeTable} LIMIT {RowCount}");
        await using var reader = await cmd.ExecuteReaderAsync();

        long sum = 0;
        while (await reader.ReadAsync())
        {
            sum += reader.GetInt64(0); // id column
        }
        return sum;
    }

    // --- Materialized read (collect all rows) ---

    [Benchmark(Description = "Materialized Read - Native")]
    public async Task<List<LargeTableRow>> Native_MaterializedRead()
    {
        var results = new List<LargeTableRow>(RowCount);
        await foreach (var row in _nativeConnection.QueryAsync<LargeTableRow>(
            $"SELECT id, category, name, value, quantity, created FROM {TestDataGenerator.LargeTable} LIMIT {RowCount}"))
        {
            results.Add(row);
        }
        return results;
    }

    [Benchmark(Description = "Materialized Read - Driver")]
    public async Task<List<LargeTableRow>> Driver_MaterializedRead()
    {
        using var cmd = _driverConnection.CreateCommand();
        cmd.CommandText = $"SELECT id, category, name, value, quantity, created FROM {TestDataGenerator.LargeTable} LIMIT {RowCount}";
        using var reader = await cmd.ExecuteReaderAsync();

        var results = new List<LargeTableRow>(RowCount);
        while (await reader.ReadAsync())
        {
            results.Add(new LargeTableRow
            {
                Id = reader.GetInt64(0),
                Category = reader.GetString(1),
                Name = reader.GetString(2),
                Value = reader.GetDouble(3),
                Quantity = reader.GetInt32(4),
                Created = reader.GetDateTime(5)
            });
        }
        return results;
    }

    [Benchmark(Description = "Materialized Read - Octonica")]
    public async Task<List<LargeTableRow>> Octonica_MaterializedRead()
    {
        using var cmd = _octonicaConnection.CreateCommand(
            $"SELECT id, category, name, value, quantity, created FROM {TestDataGenerator.LargeTable} LIMIT {RowCount}");
        await using var reader = await cmd.ExecuteReaderAsync();

        var results = new List<LargeTableRow>(RowCount);
        while (await reader.ReadAsync())
        {
            results.Add(new LargeTableRow
            {
                Id = reader.GetInt64(0),
                Category = reader.GetString(1),
                Name = reader.GetString(2),
                Value = reader.GetDouble(3),
                Quantity = reader.GetInt32(4),
                Created = reader.GetDateTime(5)
            });
        }
        return results;
    }
}
