using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pre-fix <see cref="ClickHouseDataSource.DisposeAsync"/> drained the idle
/// stack without waiting for the captured <c>PrewarmTask</c>. A prewarm rent
/// that completed the gate-acquire before <c>_disposeCts.Cancel()</c>
/// registered could push a freshly-opened connection onto _idle <em>after</em>
/// the drain loop had moved on, leaking the entry permanently.
///
/// This test stresses the race by spinning up a data source with a non-trivial
/// MinPoolSize and disposing immediately. Statistics after dispose-await must
/// be zero on every counter — any leaked entry would show as Total > 0.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class PrewarmDisposeRaceTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public PrewarmDisposeRaceTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task DisposeImmediatelyAfterConstruction_LeavesNoLeakedEntries()
    {
        var settings = _fx.BuildSettings();

        for (int i = 0; i < 10; i++)
        {
            var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
            {
                Settings = settings,
                MinPoolSize = 4,
                MaxPoolSize = 8,
                PrewarmOnStart = true,
                ConnectionWaitTimeout = TimeSpan.FromSeconds(2),
            });

            // Dispose without giving prewarm a chance to settle. The fix's
            // DisposeAsync awaits PrewarmTask before draining _idle, so any
            // freshly-opened entry is observed and discarded cleanly.
            await ds.DisposeAsync();

            var stats = ds.GetStatistics();
            _output.WriteLine($"iter {i}: Total={stats.Total}, Idle={stats.Idle}, Busy={stats.Busy}");
            Assert.Equal(0, stats.Total);
            Assert.Equal(0, stats.Idle);
            Assert.Equal(0, stats.Busy);
        }
    }

    [Fact]
    public async Task DisposeAfterPrewarmCompletes_BehavesIdentically()
    {
        // Sanity: when the prewarm has fully run before dispose, the same
        // assertions hold. Catches any regression that might break the slow
        // path while fixing the fast one.
        var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MinPoolSize = 2,
            MaxPoolSize = 4,
            PrewarmOnStart = true,
        });
        await ds.PrewarmTask;
        await ds.DisposeAsync();

        var stats = ds.GetStatistics();
        Assert.Equal(0, stats.Total);
    }
}
