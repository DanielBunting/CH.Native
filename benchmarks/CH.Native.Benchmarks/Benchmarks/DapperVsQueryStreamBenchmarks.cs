using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using CH.Native.Dapper;
using Dapper;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using NativeAdoConnection = CH.Native.Ado.ClickHouseDbConnection;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Compares typed row materialisation across:
///   - CH.Native's native streaming path (<c>QueryStreamAsync&lt;T&gt;</c>)
///   - Dapper over CH.Native's ADO.NET layer (buffered and unbuffered)
///   - Dapper over ClickHouse.Driver's ADO.NET layer (buffered and unbuffered)
///
/// Targets the existing benchmark tables: 100 rows (<c>SimpleTable</c>) and
/// 1M rows (<c>LargeTable</c>). The <c>LargeTable</c> schema differs slightly
/// from <c>SimpleTable</c>, so each path projects only the four common columns.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
[MemoryDiagnoser]
public class DapperVsQueryStreamBenchmarks
{
    private NativeConnection _nativeConnection = null!;
    private NativeAdoConnection _nativeAdoConnection = null!;
    private DriverConnection _driverConnection = null!;

    private const string SmallSql =
        "SELECT id, name, value, created FROM " + TestDataGenerator.SimpleTable;

    private const string LargeSql =
        "SELECT id, name, value, created FROM " + TestDataGenerator.LargeTable;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();
        await TestDataGenerator.EnsureTablesCreatedAsync();

        ClickHouseDapperIntegration.Register();

        var manager = BenchmarkContainerManager.Instance;

        _nativeConnection = new NativeConnection(manager.NativeConnectionString);
        await _nativeConnection.OpenAsync();

        _nativeAdoConnection = new NativeAdoConnection(manager.NativeConnectionString);
        await _nativeAdoConnection.OpenAsync();

        _driverConnection = new DriverConnection(manager.DriverConnectionString);
        await _driverConnection.OpenAsync();
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        await _nativeConnection.DisposeAsync();
        await _nativeAdoConnection.DisposeAsync();
        await _driverConnection.DisposeAsync();
    }

    // --- 100 rows (small result set) ---

    [Benchmark(Description = "100 rows - Native QueryStreamAsync<T>")]
    public async Task<int> Native_QueryStream_Small()
    {
        int count = 0;
        await foreach (var row in _nativeConnection.QueryStreamAsync<SimpleRow>(SmallSql))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "100 rows - Native Dapper QueryAsync (buffered)")]
    public async Task<int> NativeDapper_QueryAsync_Buffered_Small()
    {
        var rows = await _nativeAdoConnection.QueryAsync<SimpleRow>(SmallSql);
        return rows.Count();
    }

    [Benchmark(Description = "100 rows - Native Dapper QueryAsync (unbuffered)")]
    public async Task<int> NativeDapper_QueryAsync_Unbuffered_Small()
    {
        int count = 0;
        var rows = await _nativeAdoConnection.QueryAsync<SimpleRow>(
            new CommandDefinition(SmallSql, flags: CommandFlags.None));
        foreach (var _ in rows)
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "100 rows - Driver Dapper QueryAsync (buffered)")]
    public async Task<int> DriverDapper_QueryAsync_Buffered_Small()
    {
        var rows = await _driverConnection.QueryAsync<SimpleRow>(SmallSql);
        return rows.Count();
    }

    [Benchmark(Description = "100 rows - Driver Dapper QueryAsync (unbuffered)")]
    public async Task<int> DriverDapper_QueryAsync_Unbuffered_Small()
    {
        int count = 0;
        var rows = await _driverConnection.QueryAsync<SimpleRow>(
            new CommandDefinition(SmallSql, flags: CommandFlags.None));
        foreach (var _ in rows)
        {
            count++;
        }
        return count;
    }

    // --- 1M rows (large result set) ---

    [Benchmark(Description = "1M rows - Native QueryStreamAsync<T>")]
    public async Task<int> Native_QueryStream_Large()
    {
        int count = 0;
        await foreach (var row in _nativeConnection.QueryStreamAsync<SimpleRow>(LargeSql))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "1M rows - Native Dapper QueryAsync (buffered)")]
    public async Task<int> NativeDapper_QueryAsync_Buffered_Large()
    {
        var rows = await _nativeAdoConnection.QueryAsync<SimpleRow>(LargeSql);
        return rows.Count();
    }

    [Benchmark(Description = "1M rows - Native Dapper QueryAsync (unbuffered)")]
    public async Task<int> NativeDapper_QueryAsync_Unbuffered_Large()
    {
        int count = 0;
        var rows = await _nativeAdoConnection.QueryAsync<SimpleRow>(
            new CommandDefinition(LargeSql, flags: CommandFlags.None));
        foreach (var _ in rows)
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "1M rows - Driver Dapper QueryAsync (buffered)")]
    public async Task<int> DriverDapper_QueryAsync_Buffered_Large()
    {
        var rows = await _driverConnection.QueryAsync<SimpleRow>(LargeSql);
        return rows.Count();
    }

    [Benchmark(Description = "1M rows - Driver Dapper QueryAsync (unbuffered)")]
    public async Task<int> DriverDapper_QueryAsync_Unbuffered_Large()
    {
        int count = 0;
        var rows = await _driverConnection.QueryAsync<SimpleRow>(
            new CommandDefinition(LargeSql, flags: CommandFlags.None));
        foreach (var _ in rows)
        {
            count++;
        }
        return count;
    }

    public sealed class SimpleRow
    {
        public long Id { get; set; }
        public string Name { get; set; } = "";
        public double Value { get; set; }
        public DateTime Created { get; set; }
    }
}
