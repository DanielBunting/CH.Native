using CH.Native.Connection;
using CH.Native.DependencyInjection;
using CH.Native.Tests.Fixtures;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Integration tests that exercise the DI-wired <see cref="ClickHouseDataSource"/>
/// under concurrent load. Verifies that the pool caps concurrent physical
/// connections at MaxPoolSize, queues overflow rents on the semaphore, and
/// correctly reports state via GetStatistics().
/// </summary>
[Collection("ClickHouse")]
public class DataSourcePoolTests
{
    private readonly ClickHouseFixture _fixture;

    public DataSourcePoolTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private ServiceProvider BuildServices(int maxPoolSize, TimeSpan? waitTimeout = null)
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ClickHouse:Host"] = _fixture.Host,
                ["ClickHouse:Port"] = _fixture.Port.ToString(),
                ["ClickHouse:Username"] = _fixture.Username,
                ["ClickHouse:Password"] = _fixture.Password,
                ["ClickHouse:Pool:MaxPoolSize"] = maxPoolSize.ToString(),
                ["ClickHouse:Pool:ConnectionWaitTimeout"] = (waitTimeout ?? TimeSpan.FromSeconds(30)).ToString(),
            })
            .Build();

        var services = new ServiceCollection();
        services.AddClickHouse(config.GetSection("ClickHouse"));
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task Parallel_Rents_SaturateAtMaxPoolSize()
    {
        const int maxPoolSize = 8;
        const int parallelism = 32;

        await using var sp = BuildServices(maxPoolSize);
        var ds = sp.GetRequiredService<ClickHouseDataSource>();

        // Hold every connection open for long enough that the pool has to block
        // overflow rents on the semaphore. Use a TCS so we release all holders
        // together after observing saturation.
        var release = new TaskCompletionSource();
        var allHeld = new TaskCompletionSource();
        var heldCount = 0;

        var workers = Enumerable.Range(0, parallelism).Select(_ => Task.Run(async () =>
        {
            await using var conn = await ds.OpenConnectionAsync();
            var n = Interlocked.Increment(ref heldCount);
            if (n == maxPoolSize) allHeld.TrySetResult();
            await release.Task;
            _ = await conn.ExecuteScalarAsync<int>("SELECT 1");
        })).ToArray();

        // Wait until MaxPoolSize connections are held simultaneously.
        await allHeld.Task.WaitAsync(TimeSpan.FromSeconds(30));

        // Give scheduler a beat so pending rents are actually parked on the semaphore.
        await Task.Delay(200);

        var stats = ds.GetStatistics();
        Assert.Equal(maxPoolSize, stats.Total);
        Assert.Equal(maxPoolSize, stats.Busy);
        Assert.True(stats.PendingWaits >= parallelism - maxPoolSize - 1,
            $"Expected ~{parallelism - maxPoolSize} pending waits, saw {stats.PendingWaits}");

        release.SetResult();
        await Task.WhenAll(workers);

        var final = ds.GetStatistics();
        Assert.True(final.Busy == 0, $"Busy should drain to 0; saw {final.Busy}");
        Assert.True(final.TotalRentsServed >= parallelism,
            $"Expected >= {parallelism} rents served; saw {final.TotalRentsServed}");
        // Only MaxPoolSize physical connections should have ever been created.
        Assert.Equal(maxPoolSize, final.TotalCreated);
    }

    [Fact]
    public async Task Parallel_Rents_AllSucceed_WhenUnderMaxPoolSize()
    {
        const int maxPoolSize = 16;
        const int parallelism = 100;

        await using var sp = BuildServices(maxPoolSize);
        var ds = sp.GetRequiredService<ClickHouseDataSource>();

        // Short-lived rents — each worker grabs a connection, runs a query, releases.
        // Because work is short, the pool should serve all requests with far fewer
        // than `parallelism` physical connections via reuse.
        var successes = 0;
        await Parallel.ForEachAsync(
            Enumerable.Range(0, parallelism),
            new ParallelOptions { MaxDegreeOfParallelism = parallelism },
            async (i, ct) =>
            {
                await using var conn = await ds.OpenConnectionAsync(ct);
                // Named cancellationToken arg avoids the (string, object parameters, CancellationToken)
                // extension overload binding `ct` as a parameters object.
                var result = await conn.ExecuteScalarAsync<int>($"SELECT {i}", cancellationToken: ct);
                Assert.Equal(i, result);
                Interlocked.Increment(ref successes);
            });

        Assert.Equal(parallelism, successes);

        var stats = ds.GetStatistics();
        Assert.Equal(parallelism, stats.TotalRentsServed);
        Assert.True(stats.Total <= maxPoolSize, $"Total {stats.Total} exceeded MaxPoolSize {maxPoolSize}");
        Assert.True(stats.TotalCreated <= maxPoolSize,
            $"TotalCreated {stats.TotalCreated} should not exceed MaxPoolSize {maxPoolSize}");
    }

    [Fact]
    public async Task Rent_ThrowsTimeout_WhenPoolExhaustedBeyondWaitTimeout()
    {
        const int maxPoolSize = 2;
        var waitTimeout = TimeSpan.FromMilliseconds(500);

        await using var sp = BuildServices(maxPoolSize, waitTimeout);
        var ds = sp.GetRequiredService<ClickHouseDataSource>();

        // Hold every slot.
        var c1 = await ds.OpenConnectionAsync();
        var c2 = await ds.OpenConnectionAsync();

        try
        {
            await Assert.ThrowsAsync<TimeoutException>(async () =>
            {
                await using var _ = await ds.OpenConnectionAsync();
            });
        }
        finally
        {
            await c1.DisposeAsync();
            await c2.DisposeAsync();
        }
    }

    [Theory]
    [InlineData(4, 20)]
    [InlineData(16, 100)]
    [InlineData(32, 200)]
    public async Task Parallel_Rents_Throughput_Matrix(int maxPoolSize, int parallelism)
    {
        // Records throughput for different pool sizes. Not an assertion-heavy
        // test — it exists to make the practical parallel ceiling visible in
        // test output and to catch regressions where the pool's gating breaks.
        await using var sp = BuildServices(maxPoolSize);
        var ds = sp.GetRequiredService<ClickHouseDataSource>();

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await Parallel.ForEachAsync(
            Enumerable.Range(0, parallelism),
            new ParallelOptions { MaxDegreeOfParallelism = parallelism },
            async (_, ct) =>
            {
                await using var conn = await ds.OpenConnectionAsync(ct);
                _ = await conn.ExecuteScalarAsync<int>("SELECT 1", cancellationToken: ct);
            });
        sw.Stop();

        var stats = ds.GetStatistics();
        var opsPerSec = parallelism / sw.Elapsed.TotalSeconds;
        Console.WriteLine(
            $"[Pool max={maxPoolSize} parallelism={parallelism}] " +
            $"{sw.ElapsedMilliseconds} ms, {opsPerSec:F0} ops/s, " +
            $"created={stats.TotalCreated}, served={stats.TotalRentsServed}");

        Assert.Equal(parallelism, stats.TotalRentsServed);
        Assert.True(stats.TotalCreated <= maxPoolSize);
    }
}
