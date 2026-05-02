using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pre-fix <see cref="ClickHouseDataSource"/> evicted expired pool entries
/// only on the next <c>OpenConnectionAsync</c>. A pool that goes hours
/// without a rent kept stale sockets open. The sweeper introduced in Round 6
/// walks _idle on a periodic schedule and discards expired entries; this
/// test pins that contract by configuring a tiny lifetime and verifying the
/// idle count drops without any rent.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class PoolBackgroundEvictionTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public PoolBackgroundEvictionTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task IdleEntries_PastLifetime_AreEvictedWithoutRent()
    {
        var settings = _fx.BuildSettings();

        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = settings,
            MinPoolSize = 2,
            MaxPoolSize = 4,
            // Very short lifetime so the sweeper has something to do.
            // The sweeper cadence is min(lifetime, idle)/4 clamped to ≥ 1s.
            ConnectionLifetime = TimeSpan.FromSeconds(2),
            ConnectionIdleTimeout = TimeSpan.FromSeconds(2),
            PrewarmOnStart = true,
        });

        await ds.PrewarmTask;
        var afterPrewarm = ds.GetStatistics();
        _output.WriteLine($"after prewarm: Total={afterPrewarm.Total} Idle={afterPrewarm.Idle}");
        // Prewarm reuses a single connection across MinPoolSize iterations
        // (each iteration's `using` returns the connection before the next
        // pops it back), so the assertion is "at least one idle".
        Assert.True(afterPrewarm.Idle >= 1, "prewarm should have seeded at least one idle entry");

        // Wait long enough for at least one sweep tick after the lifetime
        // boundary. Lifetime is 2s, sweeper cadence floors at 1s, so 4 s is
        // ample.
        await Task.Delay(TimeSpan.FromSeconds(4));

        var afterSweep = ds.GetStatistics();
        _output.WriteLine($"after sweep window: Total={afterSweep.Total} Idle={afterSweep.Idle} Evicted={afterSweep.TotalEvicted}");
        Assert.True(afterSweep.Idle == 0,
            $"sweeper should have evicted all expired idle entries; saw {afterSweep.Idle} still idle");
        Assert.True(afterSweep.TotalEvicted >= 1,
            $"TotalEvicted should reflect every swept entry; saw {afterSweep.TotalEvicted}");
    }
}
