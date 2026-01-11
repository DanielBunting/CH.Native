using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using ClickHouse.Client.ADO;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using HttpConnection = ClickHouse.Client.ADO.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks for connection establishment overhead.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class ConnectionBenchmarks
{
    private string _nativeConnectionString = null!;
    private string _httpConnectionString = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();

        var manager = BenchmarkContainerManager.Instance;
        _nativeConnectionString = manager.NativeConnectionString;
        _httpConnectionString = manager.HttpConnectionString;
    }

    // --- Connection establishment ---

    [Benchmark(Description = "Connection Open - Native")]
    public async Task Native_ConnectionOpen()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
    }

    [Benchmark(Description = "Connection Open - HTTP")]
    public async Task Http_ConnectionOpen()
    {
        using var connection = new HttpConnection(_httpConnectionString);
        await connection.OpenAsync();
    }

    // --- Connection + Single Query (full round trip) ---

    [Benchmark(Description = "Connection + Query - Native")]
    public async Task<int> Native_ConnectionAndQuery()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
        return await connection.ExecuteScalarAsync<int>("SELECT 1");
    }

    [Benchmark(Description = "Connection + Query - HTTP")]
    public async Task<object?> Http_ConnectionAndQuery()
    {
        using var connection = new HttpConnection(_httpConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1";
        return await cmd.ExecuteScalarAsync();
    }

    // --- Multiple queries on same connection ---

    [Benchmark(Description = "10 Sequential Queries - Native")]
    public async Task Native_MultipleQueries()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();

        for (int i = 0; i < 10; i++)
        {
            await connection.ExecuteScalarAsync<int>($"SELECT {i}");
        }
    }

    [Benchmark(Description = "10 Sequential Queries - HTTP")]
    public async Task Http_MultipleQueries()
    {
        using var connection = new HttpConnection(_httpConnectionString);
        await connection.OpenAsync();

        for (int i = 0; i < 10; i++)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT {i}";
            await cmd.ExecuteScalarAsync();
        }
    }
}
