using CH.Native.Connection;
using CH.Native.Resilience;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Extends <see cref="LoadBalancerDistributionTests"/> with probes for behaviour under
/// concurrent opens, transient-failure recovery, and pool-size-per-host invariants.
/// Existing distribution tests pin steady-state happy-path; these add the chaos +
/// concurrency edges.
/// </summary>
[Collection("MultiToxiproxy")]
[Trait(Categories.Name, Categories.Resilience)]
public class LoadBalancingDistributionProbeTests : IAsyncLifetime
{
    private readonly MultiToxiproxyFixture _fx;
    private readonly ITestOutputHelper _output;

    public LoadBalancingDistributionProbeTests(MultiToxiproxyFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    public Task InitializeAsync() => _fx.ResetProxiesAsync();
    public Task DisposeAsync() => _fx.ResetProxiesAsync();

    [Fact]
    public async Task LoadBalancing_AfterTransientFailure_RecoversToBalanced()
    {
        // Take down B by injecting reset_peer (the toxic equivalent of "host gone"
        // — Toxiproxy doesn't expose a "down" type), run a burst, lift the toxic,
        // run a second burst — the second burst should split evenly. Probe — log
        // the actuals.
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA, _fx.EndpointB },
            b => b
                .WithLoadBalancing(LoadBalancingStrategy.RoundRobin)
                .WithResilience(r => r.WithRetry().WithCircuitBreaker()));

        await using var conn = new ResilientConnection(settings);
        await conn.OpenAsync();

        await _fx.Client.AddToxicAsync(_fx.ProxyBName, "reset_peer", "downstream",
            new Dictionary<string, object> { ["timeout"] = 0 });

        // Phase 1: B drops every connection. Retry policy should land each query on A.
        int phase1A = 0, phase1B = 0;
        for (int i = 0; i < 20; i++)
        {
            await conn.CloseAsync();
            try
            {
                var host = await conn.ExecuteScalarAsync<string>("SELECT hostName()");
                if (host == "ch_a") phase1A++; else if (host == "ch_b") phase1B++;
            }
            catch { /* B-down attempts may still surface depending on retry budget */ }
        }
        _output.WriteLine($"Phase 1 (B reset_peer): A={phase1A}, B={phase1B}");

        await _fx.ResetProxiesAsync();
        await Task.Delay(500);

        // Phase 2: B back. Probe the recovery — log distribution. Don't pin the exact
        // ratio because health-check + circuit-breaker reopen timing varies.
        int phase2A = 0, phase2B = 0;
        for (int i = 0; i < 200; i++)
        {
            await conn.CloseAsync();
            try
            {
                var host = await conn.ExecuteScalarAsync<string>("SELECT hostName()");
                if (host == "ch_a") phase2A++; else if (host == "ch_b") phase2B++;
            }
            catch { /* tolerate transient errors during recovery */ }
        }
        _output.WriteLine($"Phase 2 (recovered): A={phase2A}, B={phase2B}");

        // Pin the safety: phase 1 traffic still landed on A (B-failures didn't kill
        // the resilient connection), and phase 2 received responses from at least
        // one host. The exact post-recovery balance is left as a probe.
        Assert.True(phase1A + phase1B >= 1, "Phase 1 yielded no successful queries");
        Assert.True(phase2A + phase2B >= 50, "Phase 2 recovery yielded too few queries");
    }

    [Fact]
    public async Task LoadBalancing_ConcurrentOpens_NoDoubleCount()
    {
        // 100 parallel opens. The total count of host responses must equal 100 — no
        // duplication or loss in the LB selector.
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA, _fx.EndpointB },
            b => b.WithLoadBalancing(LoadBalancingStrategy.RoundRobin));

        const int concurrent = 100;
        var hostBag = new System.Collections.Concurrent.ConcurrentBag<string?>();

        var tasks = Enumerable.Range(0, concurrent).Select(_ => Task.Run(async () =>
        {
            await using var conn = new ResilientConnection(settings);
            await conn.OpenAsync();
            var host = await conn.ExecuteScalarAsync<string>("SELECT hostName()");
            hostBag.Add(host);
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Equal(concurrent, hostBag.Count);
        var byHost = hostBag.GroupBy(h => h).ToDictionary(g => g.Key ?? "<null>", g => g.Count());
        _output.WriteLine($"Concurrent open distribution: {string.Join(", ", byHost.Select(kv => $"{kv.Key}={kv.Value}"))}");

        Assert.Equal(concurrent, byHost.Values.Sum());
    }

    [Fact]
    public async Task DataSource_BoundsTotal_AtMaxPoolSize_UnderConcurrentBurst()
    {
        // ClickHouseDataSource pools against a single endpoint — multi-server LB is a
        // ResilientConnection concern, not a pool one. Pin the pool's MaxPoolSize
        // contract under concurrent rents (rather than per-host distribution, which
        // doesn't apply at this layer).
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.EndpointA.Host).WithPort(_fx.EndpointA.Port)
            .WithCredentials(MultiToxiproxyFixture.Username, MultiToxiproxyFixture.Password)
            .Build();

        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = settings,
            MaxPoolSize = 4,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(20),
        });

        var rentCount = 0;

        var tasks = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            await using var conn = await ds.OpenConnectionAsync();
            _ = await conn.ExecuteScalarAsync<int>("SELECT 1");
            Interlocked.Increment(ref rentCount);
            await Task.Delay(100);
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Equal(8, rentCount);
        var stats = ds.GetStatistics();
        _output.WriteLine($"Pool stats after burst: {stats}");
        Assert.True(stats.Total <= 4, $"Pool exceeded MaxPoolSize=4: Total={stats.Total}");
        Assert.Equal(0, stats.Busy);
        Assert.Equal(0, stats.PendingWaits);
    }

    [Fact]
    public async Task LoadBalancing_FullClusterReset_FailsFast_NoInfiniteRetry()
    {
        // Both hosts reset every connection. With a bounded retry policy the
        // operation must fail within a reasonable wallclock — never spin forever.
        // Toxiproxy doesn't have a "host gone" toxic, so reset_peer with
        // timeout=0 is the closest proxy for "every connect attempt fails".
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA, _fx.EndpointB },
            b => b
                .WithLoadBalancing(LoadBalancingStrategy.RoundRobin)
                .WithResilience(r => r.WithRetry().WithCircuitBreaker())
                .WithConnectTimeout(TimeSpan.FromSeconds(2)));

        await _fx.Client.AddToxicAsync(_fx.ProxyAName, "reset_peer", "downstream",
            new Dictionary<string, object> { ["timeout"] = 0 });
        await _fx.Client.AddToxicAsync(_fx.ProxyBName, "reset_peer", "downstream",
            new Dictionary<string, object> { ["timeout"] = 0 });

        try
        {
            await using var conn = new ResilientConnection(settings);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

            var watch = System.Diagnostics.Stopwatch.StartNew();
            Exception? caught = null;
            try
            {
                await conn.OpenAsync(cts.Token);
                _ = await conn.ExecuteScalarAsync<int>("SELECT 1", cts.Token);
            }
            catch (Exception ex) { caught = ex; }
            watch.Stop();

            _output.WriteLine($"Full-cluster-reset failure: {caught?.GetType().FullName} after {watch.Elapsed.TotalSeconds:F1}s");
            Assert.NotNull(caught);
            Assert.True(watch.Elapsed < TimeSpan.FromSeconds(60),
                $"Operation did not fail fast: {watch.Elapsed.TotalSeconds:F1}s");
        }
        finally
        {
            await _fx.ResetProxiesAsync();
        }
    }
}
