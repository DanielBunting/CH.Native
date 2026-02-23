using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;
using OctonicaConnection = Octonica.ClickHouseClient.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks for connection establishment overhead.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class ConnectionBenchmarks
{
    private string _nativeConnectionString = null!;
    private string _driverConnectionString = null!;
    private string _octonicaConnectionString = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();

        var manager = BenchmarkContainerManager.Instance;
        _nativeConnectionString = manager.NativeConnectionString;
        _driverConnectionString = manager.DriverConnectionString;
        _octonicaConnectionString = manager.OctonicaConnectionString;
    }

    // --- Connection establishment ---

    [Benchmark(Description = "Connection Open - Native")]
    public async Task Native_ConnectionOpen()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
    }

    [Benchmark(Description = "Connection Open - Driver")]
    public async Task Driver_ConnectionOpen()
    {
        await using var connection = new DriverConnection(_driverConnectionString);
        await connection.OpenAsync();
    }

    [Benchmark(Description = "Connection Open - Octonica")]
    public async Task Octonica_ConnectionOpen()
    {
        await using var connection = new OctonicaConnection(_octonicaConnectionString);
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

    [Benchmark(Description = "Connection + Query - Driver")]
    public async Task<object?> Driver_ConnectionAndQuery()
    {
        await using var connection = new DriverConnection(_driverConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1";
        return await cmd.ExecuteScalarAsync();
    }

    [Benchmark(Description = "Connection + Query - Octonica")]
    public async Task<object?> Octonica_ConnectionAndQuery()
    {
        await using var connection = new OctonicaConnection(_octonicaConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand("SELECT 1");
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

    [Benchmark(Description = "10 Sequential Queries - Driver")]
    public async Task Driver_MultipleQueries()
    {
        await using var connection = new DriverConnection(_driverConnectionString);
        await connection.OpenAsync();

        for (int i = 0; i < 10; i++)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT {i}";
            await cmd.ExecuteScalarAsync();
        }
    }

    [Benchmark(Description = "10 Sequential Queries - Octonica")]
    public async Task Octonica_MultipleQueries()
    {
        await using var connection = new OctonicaConnection(_octonicaConnectionString);
        await connection.OpenAsync();

        for (int i = 0; i < 10; i++)
        {
            using var cmd = connection.CreateCommand($"SELECT {i}");
            await cmd.ExecuteScalarAsync();
        }
    }
}
