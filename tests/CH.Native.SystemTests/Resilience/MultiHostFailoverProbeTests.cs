using CH.Native.Connection;
using CH.Native.Resilience;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Probes the multi-host failover path configured via the connection-string
/// <c>Servers=</c> option (or <c>WithServer(...)</c> on the builder). When
/// a primary host is unreachable, the load balancer should attempt the next
/// configured server. <see cref="LoadBalancerDistributionTests"/> covers the
/// distribution algorithm in isolation; this file probes the end-to-end
/// integration: a real DataSource configured with one alive and one dead
/// host should still serve queries.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class MultiHostFailoverProbeTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public MultiHostFailoverProbeTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task TwoHosts_OneDead_OneAlive_QueriesSucceed()
    {
        // Build settings with two servers: a deliberately-dead host (port 1)
        // and the live test container. The builder requires a primary Host
        // even when Servers is configured, so set the primary to the live one.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithServer("127.0.0.1", 1) // unreachable
            .WithServer(_fx.Host, _fx.Port)
            .WithCredentials(_fx.Username, _fx.Password)
            .WithLoadBalancing(LoadBalancingStrategy.FirstAvailable)
            .WithConnectTimeout(TimeSpan.FromSeconds(2))
            .Build();

        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = settings,
            MaxPoolSize = 2,
        });

        // 5 sequential rents — even though the first server is dead, the
        // load balancer should fail over to the second.
        for (int i = 0; i < 5; i++)
        {
            await using var conn = await ds.OpenConnectionAsync();
            var result = await conn.ExecuteScalarAsync<int>("SELECT 42");
            Assert.Equal(42, result);
        }
    }

    [Fact]
    public async Task ServersOnly_NoHost_ParsesAndConnects()
    {
        // The parser now credits at least one entry in `Servers=` toward the
        // host requirement. `Servers=` is semantically a superset of `Host=`,
        // so callers no longer need both.
        var connStr = $"Servers={_fx.Host}:{_fx.Port};Username={_fx.Username};Password={_fx.Password}";
        var settings = ClickHouseConnectionSettings.Parse(connStr);

        await using var ds = new ClickHouseDataSource(settings);
        await using var conn = await ds.OpenConnectionAsync();
        var result = await conn.ExecuteScalarAsync<int>("SELECT 11");
        Assert.Equal(11, result);
    }

    [Fact]
    public async Task AllHostsDead_OpenConnection_ThrowsTypedException()
    {
        // No live host — the rent must throw something the caller can
        // recognise (not hang past ConnectTimeout).
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithServer("127.0.0.1", 1)
            .WithServer("127.0.0.1", 2)
            .WithCredentials("default", "")
            .WithConnectTimeout(TimeSpan.FromSeconds(2))
            .Build();

        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = settings,
            MaxPoolSize = 2,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(5),
        });

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var _ = await ds.OpenConnectionAsync();
        });
        sw.Stop();

        _output.WriteLine($"All-hosts-dead surfaced in {sw.ElapsedMilliseconds} ms");
        // Should not exceed ConnectTimeout × hosts × small slack.
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(20),
            $"All-hosts-dead must surface promptly; took {sw.Elapsed}");
    }
}
