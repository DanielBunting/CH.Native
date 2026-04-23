using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using CH.Native.Connection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Pool throughput benchmark. Answers the practical question: for a given
/// MaxPoolSize, how does throughput scale as more callers contend for rents?
///
/// Each iteration fires <see cref="Parallelism"/> concurrent SELECT 1 calls
/// through a shared <see cref="ClickHouseDataSource"/>. When Parallelism
/// exceeds MaxPoolSize, overflow rents queue on the pool's semaphore — the
/// benchmark measures the end-to-end wall time for all of them to complete.
///
/// Reads should be interpreted as: "this is how long N concurrent workers
/// wait when the pool is capped at M physical connections." The ratio
/// Parallelism:MaxPoolSize controls whether the bottleneck is the pool,
/// the server, or the client.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class DataSourcePoolBenchmarks
{
    [Params(10, 50, 100, 200)]
    public int MaxPoolSize { get; set; }

    [Params(50, 200, 1000)]
    public int Parallelism { get; set; }

    private ClickHouseDataSource _dataSource = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();

        var settings = ClickHouseConnectionSettings.Parse(
            BenchmarkContainerManager.Instance.NativeConnectionString);

        _dataSource = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = settings,
            MaxPoolSize = MaxPoolSize,
            MinPoolSize = Math.Min(MaxPoolSize, 10),
            PrewarmOnStart = true,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(60),
        });

        // Prime the pool by running a batch so all physical connections exist before
        // the measured iterations. Otherwise the first iteration pays creation cost
        // and skews the distribution.
        await FireBatchAsync();
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        await _dataSource.DisposeAsync();
    }

    [Benchmark(Description = "Pool: parallel SELECT 1")]
    public Task ParallelSelect1() => FireBatchAsync();

    private Task FireBatchAsync()
    {
        return Parallel.ForEachAsync(
            Enumerable.Range(0, Parallelism),
            new ParallelOptions { MaxDegreeOfParallelism = Parallelism },
            async (_, ct) =>
            {
                await using var conn = await _dataSource.OpenConnectionAsync(ct);
                // Named arg avoids binding `ct` as the overload's `object parameters`.
                _ = await conn.ExecuteScalarAsync<int>("SELECT 1", cancellationToken: ct);
            });
    }
}
