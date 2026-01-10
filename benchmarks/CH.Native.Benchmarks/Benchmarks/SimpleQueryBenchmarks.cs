using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using ClickHouse.Client.ADO;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using HttpConnection = ClickHouse.Client.ADO.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks for simple query operations - latency baseline.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class SimpleQueryBenchmarks
{
    private NativeConnection _nativeConnection = null!;
    private HttpConnection _httpConnection = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();
        await TestDataGenerator.EnsureTablesCreatedAsync();

        var manager = BenchmarkContainerManager.Instance;

        // Native connection (CH.Native)
        _nativeConnection = new NativeConnection(manager.NativeConnectionString);
        await _nativeConnection.OpenAsync();

        // HTTP connection (ClickHouse.Client)
        _httpConnection = new HttpConnection(manager.HttpConnectionString);
        await _httpConnection.OpenAsync();
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        await _nativeConnection.DisposeAsync();
        _httpConnection.Dispose();
    }

    // --- SELECT 1 (Latency Baseline) ---

    [Benchmark(Description = "SELECT 1 - Native")]
    public async Task<int> Native_Select1()
    {
        return await _nativeConnection.ExecuteScalarAsync<int>("SELECT 1");
    }

    [Benchmark(Description = "SELECT 1 - HTTP")]
    public async Task<object?> Http_Select1()
    {
        using var cmd = _httpConnection.CreateCommand();
        cmd.CommandText = "SELECT 1";
        return await cmd.ExecuteScalarAsync();
    }

    // --- SELECT count(*) from large table ---

    [Benchmark(Description = "COUNT(*) 1M rows - Native")]
    public async Task<long> Native_CountLarge()
    {
        return await _nativeConnection.ExecuteScalarAsync<long>(
            $"SELECT count() FROM {TestDataGenerator.LargeTable}");
    }

    [Benchmark(Description = "COUNT(*) 1M rows - HTTP")]
    public async Task<object?> Http_CountLarge()
    {
        using var cmd = _httpConnection.CreateCommand();
        cmd.CommandText = $"SELECT count() FROM {TestDataGenerator.LargeTable}";
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

    [Benchmark(Description = "SELECT 100 rows - HTTP")]
    public async Task<int> Http_Select100()
    {
        using var cmd = _httpConnection.CreateCommand();
        cmd.CommandText = $"SELECT * FROM {TestDataGenerator.SimpleTable}";
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }
}
