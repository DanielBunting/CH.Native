using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;
using OctonicaConnection = Octonica.ClickHouseClient.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks for complex queries: aggregations, JOINs, filtering.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class ComplexQueryBenchmarks
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

    // --- GROUP BY aggregation ---

    private const string AggregationQuery = $@"
        SELECT
            category,
            region,
            count() as order_count,
            sum(amount) as total_amount,
            avg(quantity) as avg_quantity
        FROM {TestDataGenerator.ComplexTable}
        GROUP BY category, region
        ORDER BY total_amount DESC";

    [Benchmark(Description = "GROUP BY Aggregation - Native")]
    public async Task<int> Native_Aggregation()
    {
        int count = 0;
        await foreach (var row in _nativeConnection.QueryAsync(AggregationQuery))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "GROUP BY Aggregation - Driver")]
    public async Task<int> Driver_Aggregation()
    {
        using var cmd = _driverConnection.CreateCommand();
        cmd.CommandText = AggregationQuery;
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "GROUP BY Aggregation - Octonica")]
    public async Task<int> Octonica_Aggregation()
    {
        using var cmd = _octonicaConnection.CreateCommand(AggregationQuery);
        await using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }

    // --- Filtered query with WHERE clause ---

    private const string FilteredQuery = $@"
        SELECT *
        FROM {TestDataGenerator.ComplexTable}
        WHERE category = 'Electronics'
          AND region = 'North'
          AND quantity > 50
        LIMIT 10000";

    [Benchmark(Description = "Filtered Query - Native")]
    public async Task<int> Native_FilteredQuery()
    {
        int count = 0;
        await foreach (var row in _nativeConnection.QueryAsync(FilteredQuery))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "Filtered Query - Driver")]
    public async Task<int> Driver_FilteredQuery()
    {
        using var cmd = _driverConnection.CreateCommand();
        cmd.CommandText = FilteredQuery;
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "Filtered Query - Octonica")]
    public async Task<int> Octonica_FilteredQuery()
    {
        using var cmd = _octonicaConnection.CreateCommand(FilteredQuery);
        await using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }

    // --- Self-JOIN query ---

    private const string JoinQuery = $@"
        SELECT
            a.category,
            count(DISTINCT a.user_id) as unique_users,
            sum(a.amount) as total_amount
        FROM {TestDataGenerator.ComplexTable} a
        INNER JOIN (
            SELECT user_id, max(amount) as max_amount
            FROM {TestDataGenerator.ComplexTable}
            GROUP BY user_id
        ) b ON a.user_id = b.user_id AND a.amount = b.max_amount
        GROUP BY a.category";

    [Benchmark(Description = "JOIN Query - Native")]
    public async Task<int> Native_JoinQuery()
    {
        int count = 0;
        await foreach (var row in _nativeConnection.QueryAsync(JoinQuery))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "JOIN Query - Driver")]
    public async Task<int> Driver_JoinQuery()
    {
        using var cmd = _driverConnection.CreateCommand();
        cmd.CommandText = JoinQuery;
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "JOIN Query - Octonica")]
    public async Task<int> Octonica_JoinQuery()
    {
        using var cmd = _octonicaConnection.CreateCommand(JoinQuery);
        await using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }

    // --- ORDER BY with LIMIT ---

    private const string SortedQuery = $@"
        SELECT *
        FROM {TestDataGenerator.ComplexTable}
        ORDER BY amount DESC, created ASC
        LIMIT 1000";

    [Benchmark(Description = "Sorted Query TOP 1000 - Native")]
    public async Task<int> Native_SortedQuery()
    {
        int count = 0;
        await foreach (var row in _nativeConnection.QueryAsync(SortedQuery))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "Sorted Query TOP 1000 - Driver")]
    public async Task<int> Driver_SortedQuery()
    {
        using var cmd = _driverConnection.CreateCommand();
        cmd.CommandText = SortedQuery;
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "Sorted Query TOP 1000 - Octonica")]
    public async Task<int> Octonica_SortedQuery()
    {
        using var cmd = _octonicaConnection.CreateCommand(SortedQuery);
        await using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }
}
