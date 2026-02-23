using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;
using OctonicaConnection = Octonica.ClickHouseClient.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks for simple query operations - latency baseline.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class SimpleQueryBenchmarks
{
    private NativeConnection _nativeConnection = null!;
    private DriverConnection _driverConnection = null!;
    private OctonicaConnection _octonicaConnection = null!;

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

    // --- SELECT 1 (Latency Baseline) ---

    [Benchmark(Description = "SELECT 1 - Native")]
    public async Task<int> Native_Select1()
    {
        return await _nativeConnection.ExecuteScalarAsync<int>("SELECT 1");
    }

    [Benchmark(Description = "SELECT 1 - Driver")]
    public async Task<object?> Driver_Select1()
    {
        using var cmd = _driverConnection.CreateCommand();
        cmd.CommandText = "SELECT 1";
        return await cmd.ExecuteScalarAsync();
    }

    [Benchmark(Description = "SELECT 1 - Octonica")]
    public async Task<object?> Octonica_Select1()
    {
        using var cmd = _octonicaConnection.CreateCommand("SELECT 1");
        return await cmd.ExecuteScalarAsync();
    }

    // --- SELECT count(*) from large table ---

    [Benchmark(Description = "COUNT(*) 1M rows - Native")]
    public async Task<long> Native_CountLarge()
    {
        return await _nativeConnection.ExecuteScalarAsync<long>(
            $"SELECT count() FROM {TestDataGenerator.LargeTable}");
    }

    [Benchmark(Description = "COUNT(*) 1M rows - Driver")]
    public async Task<object?> Driver_CountLarge()
    {
        using var cmd = _driverConnection.CreateCommand();
        cmd.CommandText = $"SELECT count() FROM {TestDataGenerator.LargeTable}";
        return await cmd.ExecuteScalarAsync();
    }

    [Benchmark(Description = "COUNT(*) 1M rows - Octonica")]
    public async Task<object?> Octonica_CountLarge()
    {
        using var cmd = _octonicaConnection.CreateCommand(
            $"SELECT count() FROM {TestDataGenerator.LargeTable}");
        return await cmd.ExecuteScalarAsync();
    }

    // --- SELECT * with small result (100 rows) ---

    [Benchmark(Description = "SELECT 100 rows - Native")]
    public async Task<int> Native_Select100()
    {
        int count = 0;
        await foreach (var row in _nativeConnection.QueryAsync(
            $"SELECT * FROM {TestDataGenerator.SimpleTable}"))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SELECT 100 rows - Driver")]
    public async Task<int> Driver_Select100()
    {
        using var cmd = _driverConnection.CreateCommand();
        cmd.CommandText = $"SELECT * FROM {TestDataGenerator.SimpleTable}";
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SELECT 100 rows - Octonica")]
    public async Task<int> Octonica_Select100()
    {
        using var cmd = _octonicaConnection.CreateCommand(
            $"SELECT * FROM {TestDataGenerator.SimpleTable}");
        await using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }
}
