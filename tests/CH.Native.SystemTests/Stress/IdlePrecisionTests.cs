using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Stress;

/// <summary>
/// Sanity-checks the connection pool's idle-eviction timing. Not a strict promise on
/// timing precision, but a tripwire if eviction is broken (never fires) or too eager
/// (evicts active connections).
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class IdlePrecisionTests
{
    private readonly SingleNodeFixture _fixture;

    public IdlePrecisionTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task IdleConnections_AreEventuallyEvicted()
    {
        var options = new ClickHouseDataSourceOptions
        {
            Settings = _fixture.BuildSettings(),
            MaxPoolSize = 4,
            ConnectionIdleTimeout = TimeSpan.FromMilliseconds(500),
        };
        await using var ds = new ClickHouseDataSource(options);

        // Rent + return a few times to populate the idle pool.
        for (int i = 0; i < 4; i++)
        {
            await using var c = await ds.OpenConnectionAsync();
            _ = await c.ExecuteScalarAsync<int>("SELECT 1");
        }

        var beforeCreated = ds.GetStatistics().TotalCreated;
        Assert.True(beforeCreated >= 1);

        // Wait well past the idle timeout.
        await Task.Delay(TimeSpan.FromSeconds(2));

        // After the idle timeout, the next rent must create a fresh physical
        // connection (the cached one was evicted as stale). Strict inequality —
        // `>=` would be a tautology.
        await using (var c = await ds.OpenConnectionAsync())
        {
            var v = await c.ExecuteScalarAsync<int>("SELECT 42");
            Assert.Equal(42, v);
        }

        var afterStats = ds.GetStatistics();
        Assert.True(afterStats.TotalCreated > beforeCreated,
            $"Expected a new physical connection after idle timeout (TotalCreated > {beforeCreated}); saw {afterStats.TotalCreated}.");
    }
}
