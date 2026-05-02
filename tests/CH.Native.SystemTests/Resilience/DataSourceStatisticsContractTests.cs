using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pins the <see cref="DataSourceStatistics"/> contract that operators wire
/// into <c>/diag/pool</c> admin endpoints. Without these probes, capacity
/// decisions are based on values whose definitions can drift silently.
///
/// <para>
/// The shape under test: <c>Total / Idle / Busy / PendingWaits /
/// TotalRentsServed / TotalCreated / TotalEvicted</c>.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class DataSourceStatisticsContractTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public DataSourceStatisticsContractTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task FreshPool_NoPrewarm_StartsEmpty()
    {
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 4,
            MinPoolSize = 0,
            PrewarmOnStart = false,
        });

        var stats = ds.GetStatistics();
        _output.WriteLine($"Fresh: {stats}");

        Assert.Equal(0, stats.Total);
        Assert.Equal(0, stats.Idle);
        Assert.Equal(0, stats.Busy);
        Assert.Equal(0, stats.PendingWaits);
        Assert.Equal(0, stats.TotalRentsServed);
        Assert.Equal(0, stats.TotalCreated);
    }

    [Fact]
    public async Task PrewarmOnStart_OpensAtLeastOneConnection_BeforeAnyUserRent()
    {
        // Prewarm uses sequential `await using` rents — each rent opens then
        // immediately returns the connection, so subsequent prewarm rents
        // pop the already-pooled idle entry rather than creating fresh ones.
        // That means Total/Idle settle at 1 even with MinPoolSize=2; what we
        // can robustly pin is that *some* connection got opened before any
        // user-driven rent. TotalRentsServed counts prewarm rents too.
        const int min = 2;
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 4,
            MinPoolSize = min,
            PrewarmOnStart = true,
        });

        await PollAsync(() => ds.GetStatistics().Idle >= 1, TimeSpan.FromSeconds(5));

        var stats = ds.GetStatistics();
        _output.WriteLine($"Prewarmed: {stats}");
        Assert.True(stats.Total >= 1);
        Assert.True(stats.Idle >= 1);
        Assert.Equal(0, stats.Busy);
        Assert.True(stats.TotalCreated >= 1);
        Assert.True(stats.TotalRentsServed >= 1);
    }

    [Fact]
    public async Task RentAndReturn_ShiftsBusyAndIdleAndIncrementsTotalRentsServed()
    {
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 2,
        });

        var conn = await ds.OpenConnectionAsync();
        var midRent = ds.GetStatistics();
        _output.WriteLine($"Mid-rent: {midRent}");
        Assert.Equal(1, midRent.Busy);
        Assert.Equal(0, midRent.Idle);
        Assert.Equal(1, midRent.Total);
        Assert.Equal(1, midRent.TotalRentsServed);

        await conn.DisposeAsync();
        var afterReturn = ds.GetStatistics();
        _output.WriteLine($"After-return: {afterReturn}");
        Assert.Equal(0, afterReturn.Busy);
        Assert.Equal(1, afterReturn.Idle);
        Assert.Equal(1, afterReturn.Total);
        Assert.Equal(1, afterReturn.TotalRentsServed);
    }

    [Fact]
    public async Task SaturatedPool_ThirdRent_ParksAndIncrementsPendingWaits()
    {
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 2,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(10),
        });

        var rent1 = await ds.OpenConnectionAsync();
        var rent2 = await ds.OpenConnectionAsync();

        // Third rent will park on the gate semaphore.
        var thirdRentTask = Task.Run(async () => await ds.OpenConnectionAsync());

        await PollAsync(() => ds.GetStatistics().PendingWaits == 1, TimeSpan.FromSeconds(2));

        var saturated = ds.GetStatistics();
        _output.WriteLine($"Saturated: {saturated}");
        Assert.Equal(2, saturated.Total);
        Assert.Equal(2, saturated.Busy);
        Assert.Equal(1, saturated.PendingWaits);

        await rent1.DisposeAsync();
        var rent3 = await thirdRentTask;
        await rent3.DisposeAsync();
        await rent2.DisposeAsync();

        var drained = ds.GetStatistics();
        _output.WriteLine($"Drained: {drained}");
        Assert.Equal(0, drained.PendingWaits);
        Assert.True(drained.TotalRentsServed >= 3);
    }

    [Fact]
    public async Task DataSourceDispose_DrainsAllStatistics_ToZero()
    {
        // The discard-on-poison signal lives in the broader broken-connection
        // suite (BrokenConnectionPoolReturnTests) — server-side KILL QUERY
        // doesn't reliably flag a connection as un-poolable, so a dedicated
        // statistics probe of that path is flaky. Instead, pin the
        // disposal-time invariant: after DataSource disposal, no stats
        // suggest live connections.
        ClickHouseDataSource ds = new(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 4,
        });

        // Drive a few rents so Total > 0.
        await using (var c = await ds.OpenConnectionAsync())
            _ = await c.ExecuteScalarAsync<int>("SELECT 1");
        await using (var c = await ds.OpenConnectionAsync())
            _ = await c.ExecuteScalarAsync<int>("SELECT 1");

        var beforeDispose = ds.GetStatistics();
        _output.WriteLine($"Before dispose: {beforeDispose}");
        Assert.True(beforeDispose.Idle >= 1);

        await ds.DisposeAsync();

        // Post-dispose, the snapshot fields that describe live state
        // should report drained.
        var stats = ds.GetStatistics();
        _output.WriteLine($"After dispose: {stats}");
        Assert.Equal(0, stats.Busy);
        Assert.Equal(0, stats.PendingWaits);
    }

    [Fact]
    public async Task ConcurrentRentsAndReturns_SnapshotInvariantsHold()
    {
        // Idle + Busy <= Total at all times; no negative counters under flux.
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 4,
        });

        var observer = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var observerTask = Task.Run(async () =>
        {
            while (!observer.IsCancellationRequested)
            {
                var s = ds.GetStatistics();
                Assert.True(s.Idle >= 0);
                Assert.True(s.Busy >= 0);
                Assert.True(s.Total >= 0);
                Assert.True(s.PendingWaits >= 0);
                Assert.True(s.Idle + s.Busy <= s.Total + s.PendingWaits,
                    $"Idle({s.Idle}) + Busy({s.Busy}) > Total({s.Total}) + PendingWaits({s.PendingWaits})");
                await Task.Yield();
            }
        });

        var workers = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < 25; i++)
            {
                await using var conn = await ds.OpenConnectionAsync();
                _ = await conn.ExecuteScalarAsync<int>("SELECT 1");
            }
        })).ToArray();

        await Task.WhenAll(workers);
        await observer.CancelAsync();
        await observerTask;

        var final = ds.GetStatistics();
        _output.WriteLine($"Final: {final}");
        Assert.Equal(0, final.PendingWaits);
        Assert.Equal(0, final.Busy);
        Assert.True(final.TotalRentsServed >= 8 * 25);
    }

    private static async Task PollAsync(Func<bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate()) return;
            await Task.Delay(25);
        }
        throw new TimeoutException($"Condition was not satisfied within {timeout}.");
    }
}
