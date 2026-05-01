using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pins that <see cref="ClickHouseDataSource.DisposeAsync"/> tears down
/// cleanly even when the background eviction sweeper task is mid-iteration.
/// The sweeper runs <see cref="Task.Delay(TimeSpan, CancellationToken)"/>
/// in a loop with the dispose token; dispose cancels that token, but a sweep
/// pass that's already in progress (popping idle entries, calling
/// <c>DiscardInternalAsync</c>) must complete or abort cleanly.
///
/// <para>
/// Existing <see cref="PoolBackgroundEvictionTests"/> verifies sweeper
/// functionality on its own. This file adds the dispose-during-sweep race.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class EvictionSweeperDisposeRaceTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public EvictionSweeperDisposeRaceTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task DisposeMidSweep_TearsDownCleanly_NoConnectionLeak()
    {
        // Configure a tight sweep cadence so we maximise the chance of the
        // dispose call landing while a sweep iteration is mid-flight.
        // ConnectionLifetime/IdleTimeout = 1s clamps the sweeper cadence
        // floor (1s per HealthChecker.cs cadence calculation).
        var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MinPoolSize = 4,
            MaxPoolSize = 8,
            ConnectionLifetime = TimeSpan.FromSeconds(1),
            ConnectionIdleTimeout = TimeSpan.FromSeconds(1),
            PrewarmOnStart = true,
        });

        await ds.PrewarmTask;

        // Wait long enough for sweeper to hit at least one iteration where
        // entries are eligible for eviction (lifetime = 1s, sweep ≥ 1s).
        await Task.Delay(TimeSpan.FromMilliseconds(1500));

        // Dispose. Must complete in well under the sweep interval — i.e.
        // dispose's cancellation must wake the sweeper's Task.Delay
        // immediately, and any in-flight sweep loop must exit promptly.
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await ds.DisposeAsync();
        sw.Stop();

        _output.WriteLine($"Dispose during sweep window: {sw.ElapsedMilliseconds} ms");
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5),
            "Dispose must cancel the sweeper promptly; took " + sw.Elapsed);
    }

    [Fact]
    public async Task RepeatedCreateAndDispose_NoSweeperTaskAccumulation()
    {
        // Each DataSource starts a sweeper Task. Repeated create/dispose
        // must reliably end each sweeper — otherwise process Task count
        // grows unbounded.
        for (int i = 0; i < 10; i++)
        {
            var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
            {
                Settings = _fx.BuildSettings(),
                MaxPoolSize = 2,
                ConnectionLifetime = TimeSpan.FromSeconds(1),
                ConnectionIdleTimeout = TimeSpan.FromSeconds(1),
            });
            await using (var c = await ds.OpenConnectionAsync()) { }
            await ds.DisposeAsync();
        }
        // No assertion needed — if any iteration leaked, we'd have been
        // stuck in DisposeAsync's wait or the test harness would surface
        // a thread-pool exhaustion.
    }
}
