using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.Resilience;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Plan §4 #5 — pin that the configured <c>HealthCheckInterval</c> is honoured by
/// observing recovery latency end-to-end. The library doesn't expose health-check
/// timestamps directly, but we can infer the interval by:
///   1. Knocking a host out via reset_peer.
///   2. Burning a few queries against the surviving host so the unhealthy mark is set.
///   3. Restoring the toxic.
///   4. Polling until queries land on the recovered host again, measuring elapsed.
///
/// Recovery should land within roughly 1–3 health-check intervals; if it lands much
/// later, either the interval is being ignored or the circuit-breaker reopen window
/// is dominating.
/// </summary>
[Collection("MultiToxiproxy")]
[Trait(Categories.Name, Categories.Resilience)]
public class HealthCheckTimingProbeTests : IAsyncLifetime
{
    private readonly MultiToxiproxyFixture _fx;
    private readonly ITestOutputHelper _output;

    public HealthCheckTimingProbeTests(MultiToxiproxyFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    public Task InitializeAsync() => _fx.ResetProxiesAsync();
    public Task DisposeAsync() => _fx.ResetProxiesAsync();

    [Fact]
    public async Task HealthCheckInterval_2Seconds_RecoveryLandsWithinAFewIntervals()
    {
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA, _fx.EndpointB },
            b => b
                .WithLoadBalancing(LoadBalancingStrategy.RoundRobin)
                .WithResilience(r => r
                    .WithRetry()
                    .WithCircuitBreaker()
                    .WithHealthCheckInterval(TimeSpan.FromSeconds(2)))
                .WithConnectTimeout(TimeSpan.FromSeconds(2)));

        await using var conn = new ResilientConnection(settings);
        await conn.OpenAsync();

        // Knock B out for the duration of the failed-host phase.
        await _fx.Client.AddToxicAsync(_fx.ProxyBName, "reset_peer", "downstream",
            new Dictionary<string, object> { ["timeout"] = 0 });

        // Drive enough queries that the LB definitely tries B and marks it unhealthy.
        for (int i = 0; i < 20; i++)
        {
            await conn.CloseAsync();
            try { _ = await conn.ExecuteScalarAsync<string>("SELECT hostName()"); }
            catch { /* tolerate */ }
        }

        // Restore B; from this moment until the next health-check probe succeeds,
        // queries should keep landing on A. Measure how long until B is in rotation
        // again.
        await _fx.ResetProxiesAsync();
        var watch = Stopwatch.StartNew();

        // Poll for a B-landing query. Cap at 6× interval so a regression (interval
        // ignored / pinned to 30s default) fails clearly.
        var deadline = TimeSpan.FromSeconds(15);
        TimeSpan? recoveryLatency = null;
        while (watch.Elapsed < deadline)
        {
            await conn.CloseAsync();
            try
            {
                var host = await conn.ExecuteScalarAsync<string>("SELECT hostName()");
                if (host == "ch_b")
                {
                    recoveryLatency = watch.Elapsed;
                    break;
                }
            }
            catch { /* tolerate transient errors during recovery */ }
            await Task.Delay(200);
        }
        watch.Stop();

        _output.WriteLine($"Recovery latency: {recoveryLatency?.TotalSeconds.ToString("F2") ?? "<not detected>"} s " +
                          $"(health-check interval = 2s)");

        Assert.NotNull(recoveryLatency);
        // 2s interval; allow 3× interval for first-probe-after-recovery noise.
        Assert.True(recoveryLatency!.Value < TimeSpan.FromSeconds(8),
            $"Recovery took {recoveryLatency.Value.TotalSeconds:F1}s — interval looks ignored");
    }

    [Fact]
    public async Task DrainCeiling_SlowToxic_DoesNotHangPastWallclock()
    {
        // §3 #3 — slow downstream so drain hits the ceiling. The library's drain
        // timeout caps how long we wait when closing a connection that has bytes
        // still in flight. With heavy bandwidth limiting the drain inevitably
        // bumps the cap; pin only the safety invariant: dispose returns within
        // a bounded wallclock, no infinite hang.
        // Use the singular endpoint — ClickHouseConnection (vs ResilientConnection)
        // doesn't honour multi-host LB; it always connects to settings.Host:Port.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.EndpointA.Host)
            .WithPort(_fx.EndpointA.Port)
            .WithCredentials(MultiToxiproxyFixture.Username, MultiToxiproxyFixture.Password)
            .Build();

        // Hard bandwidth cap (bytes/sec rate).
        await _fx.Client.AddToxicAsync(_fx.ProxyAName, "bandwidth", "downstream",
            new Dictionary<string, object> { ["rate"] = 1024 });

        try
        {
            // Kick off a large query, cancel mid-stream so dispose has to drain.
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            var conn = new ClickHouseConnection(settings);
            await conn.OpenAsync();

            try
            {
                await foreach (var _ in conn.QueryAsync<ulong>(
                    "SELECT number FROM numbers(10000000)").WithCancellation(cts.Token)) { }
            }
            catch { /* expected — cancellation or read failure */ }

            // The slow toxic is still active. Dispose should respect the drain
            // ceiling and return promptly — never hang past, say, 60s wallclock.
            var watch = Stopwatch.StartNew();
            await conn.DisposeAsync();
            watch.Stop();

            _output.WriteLine($"Dispose under slow drain: {watch.Elapsed.TotalSeconds:F2} s");
            Assert.True(watch.Elapsed < TimeSpan.FromSeconds(60),
                $"Dispose hung past drain ceiling: {watch.Elapsed.TotalSeconds:F1}s");
        }
        finally
        {
            await _fx.ResetProxiesAsync();
        }
    }
}
