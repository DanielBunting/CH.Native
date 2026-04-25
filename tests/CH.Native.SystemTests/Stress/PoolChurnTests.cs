using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Stress;

/// <summary>
/// Stresses the connection pool under realistic operational shapes: bursty rents,
/// permit exhaustion, validate-on-rent recovery, and dispose races.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class PoolChurnTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public PoolChurnTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task ConcurrentRents_AgainstBoundedPool_AllSucceed()
    {
        const int permits = 10;
        const int callers = 200;

        var options = new ClickHouseDataSourceOptions
        {
            Settings = _fixture.BuildSettings(),
            MaxPoolSize = permits,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(20),
        };
        await using var ds = new ClickHouseDataSource(options);

        var sw = Stopwatch.StartNew();
        var tasks = Enumerable.Range(0, callers).Select(_ => Task.Run(async () =>
        {
            await using var c = await ds.OpenConnectionAsync();
            _ = await c.ExecuteScalarAsync<int>("SELECT 1");
        })).ToArray();
        await Task.WhenAll(tasks);
        sw.Stop();

        var stats = ds.GetStatistics();
        _output.WriteLine($"{callers} rents through {permits}-permit pool in {sw.ElapsedMilliseconds}ms");
        _output.WriteLine($"  TotalRentsServed = {stats.TotalRentsServed}");
        _output.WriteLine($"  TotalCreated     = {stats.TotalCreated}");

        Assert.True(stats.TotalRentsServed >= callers);
        // Pool should have recycled rather than spawned a fresh connection per caller.
        Assert.True(stats.TotalCreated <= permits + 5,
            $"Pool created {stats.TotalCreated} physical connections (expected ~{permits}); recycling looks broken.");
    }

    [Fact]
    public async Task WaitTimeoutExpires_WhenAllPermitsHeld()
    {
        var options = new ClickHouseDataSourceOptions
        {
            Settings = _fixture.BuildSettings(),
            MaxPoolSize = 1,
            ConnectionWaitTimeout = TimeSpan.FromMilliseconds(250),
        };
        await using var ds = new ClickHouseDataSource(options);

        var holder = await ds.OpenConnectionAsync();
        try
        {
            var sw = Stopwatch.StartNew();
            await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                var c = await ds.OpenConnectionAsync();
                await c.DisposeAsync();
            });
            sw.Stop();
            // Wait timeout was 250 ms; fire window is tight. 200 ms lower bound to
            // confirm we waited near-the-budget; 1500 ms upper bound to catch hangs.
            Assert.InRange(sw.ElapsedMilliseconds, 200, 1500);
        }
        finally
        {
            await holder.DisposeAsync();
        }
    }

    [Fact]
    public async Task Prewarm_StartsCreatingConnections()
    {
        var options = new ClickHouseDataSourceOptions
        {
            Settings = _fixture.BuildSettings(),
            MinPoolSize = 4,
            MaxPoolSize = 8,
            PrewarmOnStart = true,
        };
        await using var ds = new ClickHouseDataSource(options);

        // Prewarm runs as a background fire-and-forget task; we only assert it
        // *eventually* triggers connection creation, not how aggressively. The exact
        // pacing is an implementation detail.
        var stats = default(DataSourceStatistics);
        for (int i = 0; i < 60; i++)
        {
            stats = ds.GetStatistics();
            if (stats.TotalCreated >= 1) break;
            await Task.Delay(100);
        }
        Assert.True(stats.TotalCreated >= 1,
            $"Prewarm should have created ≥ 1 connection, saw {stats.TotalCreated}.");
    }

    [Fact]
    public async Task DisposeRace_PendingRentsCancelCleanly()
    {
        var options = new ClickHouseDataSourceOptions
        {
            Settings = _fixture.BuildSettings(),
            MaxPoolSize = 1,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(30),
        };
        var ds = new ClickHouseDataSource(options);

        var holder = await ds.OpenConnectionAsync();

        // Park a rent that will never get a permit until holder is disposed.
        Exception? pendingException = null;
        var pending = Task.Run(async () =>
        {
            try
            {
                var c = await ds.OpenConnectionAsync();
                await c.DisposeAsync();
            }
            catch (Exception ex)
            {
                pendingException = ex;
            }
        });

        // Now race a Dispose on the data source itself.
        await Task.Delay(150);
        var disposeTask = ds.DisposeAsync().AsTask();
        await holder.DisposeAsync();

        await disposeTask;
        await pending;

        Assert.NotNull(pendingException);
        // Expected shape: ObjectDisposedException or OperationCanceledException
        // (depending on how the dispose path signals parked waiters). Either is fine,
        // but raw Exception or anything else suggests a sloppy dispose path.
        Assert.True(
            pendingException is ObjectDisposedException
            || pendingException is OperationCanceledException,
            $"Pending rent failed with an unexpected type: {pendingException.GetType().FullName}: {pendingException.Message}");
    }
}
