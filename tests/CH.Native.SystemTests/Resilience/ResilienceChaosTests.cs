using System.Diagnostics;
using System.Diagnostics.Metrics;
using CH.Native.Connection;
using CH.Native.Resilience;
using CH.Native.SystemTests.Fixtures;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using Xunit;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Crosses ResilientConnection (retry, circuit breaker, load balancer, health checker)
/// with the chaos fixture so the resilience features are tested under the failures
/// they exist to handle. Existing integration tests run against a healthy server.
/// </summary>
[Collection("MultiToxiproxy")]
[Trait(Categories.Name, Categories.Resilience)]
public class ResilienceChaosTests
{
    private readonly MultiToxiproxyFixture _fx;

    public ResilienceChaosTests(MultiToxiproxyFixture fx)
    {
        _fx = fx;
    }

    [Fact]
    public async Task LoadBalancer_FailsOverWhenOneEndpointBecomesUnreachable()
    {
        // Reset every TCP connection at the proxy — calls via A surface as a
        // transient ClickHouseConnectionException, which is what the LB needs
        // to observe in order to mark A failed.
        await _fx.Client.AddToxicAsync(_fx.ProxyAName, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });

        try
        {
            // Servers ordered [B, A] so round-robin's first pick (index 1) lands
            // on A — that one connect attempt is what feeds the LB its failure
            // signal. Round-robin only rotates across reconnects, so leaving A
            // last in the list would mean nothing ever attempts it.
            // Tighter HealthCheckTimeout keeps the background probe from sitting
            // on the default 5s hang against the broken endpoint.
            var settings = _fx.BuildSettings(
                new[] { _fx.EndpointB, _fx.EndpointA },
                b => b.WithLoadBalancing(LoadBalancingStrategy.RoundRobin)
                      .WithResilience(r => r
                          .WithRetry(new RetryOptions
                          {
                              MaxRetries = 3,
                              BaseDelay = TimeSpan.FromMilliseconds(50)
                          })
                          .WithHealthCheckTimeout(TimeSpan.FromMilliseconds(500))));

            await using var conn = new ResilientConnection(settings);
            await conn.OpenAsync();

            // Issue 10 queries — all should succeed (returning 1).
            for (int i = 0; i < 10; i++)
            {
                var v = await conn.ExecuteScalarAsync<int>("SELECT 1");
                Assert.Equal(1, v);
            }

            // Pin that the LB recognised A is unhealthy. With 2 endpoints and one broken,
            // HealthyServerCount must be exactly 1 — not 2 (LB blind to chaos), not 0
            // (LB also marked B failed by mistake).
            Assert.Equal(1, conn.HealthyServerCount);
        }
        finally
        {
            await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);
        }
    }

    [Fact]
    public async Task LoadBalancer_RestoresEndpointAfterChaosClears()
    {
        // Same setup as the failover test, then remove the toxic and let the
        // background probe run a few cycles. HealthyServerCount must climb back
        // to 2 — proving the LB doesn't latch unhealthy state forever once a
        // node observably recovers.
        await _fx.Client.AddToxicAsync(_fx.ProxyAName, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });

        try
        {
            var settings = _fx.BuildSettings(
                new[] { _fx.EndpointB, _fx.EndpointA },
                b => b.WithLoadBalancing(LoadBalancingStrategy.RoundRobin)
                      .WithResilience(r => r
                          .WithRetry(new RetryOptions
                          {
                              MaxRetries = 3,
                              BaseDelay = TimeSpan.FromMilliseconds(50)
                          })
                          // Tight loop so the probe has time to recover A inside
                          // the test budget.
                          .WithHealthCheckInterval(TimeSpan.FromMilliseconds(200))
                          .WithHealthCheckTimeout(TimeSpan.FromMilliseconds(500))));

            await using var conn = new ResilientConnection(settings);
            await conn.OpenAsync();

            // Sanity: A is marked unhealthy by the connect-time path.
            Assert.Equal(1, conn.HealthyServerCount);

            await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);

            // Background probe runs every 200ms after a 1s startup delay.
            // A successful probe calls MarkHealthy which flips _isHealthy back.
            // Poll for up to 5s — under nominal conditions this trips well inside 2s.
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < deadline && conn.HealthyServerCount < 2)
            {
                await Task.Delay(100);
            }

            Assert.Equal(2, conn.HealthyServerCount);
        }
        finally
        {
            await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);
        }
    }

    [Fact]
    public async Task LoadBalancer_AllEndpointsDown_HealthyCountReachesZero()
    {
        // Both endpoints rejected at the proxy. OpenAsync exhausts retries; the
        // connect-time MarkServerFailed path should have observed both servers
        // and dropped HealthyServerCount to 0.
        await _fx.Client.AddToxicAsync(_fx.ProxyAName, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });
        await _fx.Client.AddToxicAsync(_fx.ProxyBName, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });

        try
        {
            var settings = _fx.BuildSettings(
                new[] { _fx.EndpointA, _fx.EndpointB },
                b => b.WithLoadBalancing(LoadBalancingStrategy.RoundRobin)
                      .WithResilience(r => r
                          .WithRetry(new RetryOptions
                          {
                              MaxRetries = 3,
                              BaseDelay = TimeSpan.FromMilliseconds(20)
                          })
                          .WithHealthCheckTimeout(TimeSpan.FromMilliseconds(500))));

            await using var conn = new ResilientConnection(settings);

            await Assert.ThrowsAnyAsync<Exception>(() => conn.OpenAsync());

            Assert.Equal(0, conn.HealthyServerCount);
        }
        finally
        {
            await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);
            await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyBName);
        }
    }

    [Fact]
    public async Task LoadBalancer_FailsOverAfterMidFlightFailure()
    {
        // Connect to a healthy cluster, break the live endpoint mid-flight,
        // then issue a query. Reconnect attempts should bypass the failed
        // endpoint via the LB, the query should ultimately succeed against the
        // surviving node, and that node should be the new current server.
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA, _fx.EndpointB },
            b => b.WithLoadBalancing(LoadBalancingStrategy.RoundRobin)
                  .WithResilience(r => r
                      .WithRetry(new RetryOptions
                      {
                          MaxRetries = 5,
                          BaseDelay = TimeSpan.FromMilliseconds(50)
                      })
                      .WithHealthCheckTimeout(TimeSpan.FromMilliseconds(500))));

        await using var conn = new ResilientConnection(settings);
        await conn.OpenAsync();

        // Round-robin's first pick lands on B (nodes[1]).
        var initialServer = conn.CurrentServer;
        Assert.NotNull(initialServer);

        var brokenProxy = initialServer!.Value.Port == _fx.ProxyBPort
            ? _fx.ProxyBName
            : _fx.ProxyAName;

        await _fx.Client.AddToxicAsync(brokenProxy, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });

        try
        {
            var v = await conn.ExecuteScalarAsync<int>("SELECT 1");
            Assert.Equal(1, v);

            // Failover landed on the other endpoint.
            Assert.NotEqual(initialServer.Value, conn.CurrentServer);

            // At least the originally-broken endpoint is out of rotation.
            Assert.InRange(conn.HealthyServerCount, 1, 1);
        }
        finally
        {
            await _fx.Client.RemoveAllToxicsAsync(brokenProxy);
        }
    }

    [Fact]
    public async Task PostFailover_ReadsRouteToSurvivingServer()
    {
        // Server-side proof of failover: ClickHouse's hostName() function reports
        // the container's hostname ("ch_a" or "ch_b"), so a query result is the
        // ground truth for which node served it. Asserts that (a) the failed-over
        // query returns the surviving host's name, and (b) subsequent reads stay
        // pinned there rather than flapping back.
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA, _fx.EndpointB },
            b => b.WithLoadBalancing(LoadBalancingStrategy.RoundRobin)
                  .WithResilience(r => r
                      .WithRetry(new RetryOptions
                      {
                          MaxRetries = 5,
                          BaseDelay = TimeSpan.FromMilliseconds(50)
                      })
                      .WithHealthCheckTimeout(TimeSpan.FromMilliseconds(500))));

        await using var conn = new ResilientConnection(settings);
        await conn.OpenAsync();

        var initialHost = await conn.ExecuteScalarAsync<string>("SELECT hostName()");
        Assert.NotNull(initialHost);
        Assert.Contains(initialHost, new[] { "ch_a", "ch_b" });

        var brokenHost = initialHost!;
        var survivingHost = brokenHost == "ch_a" ? "ch_b" : "ch_a";
        var brokenProxy = brokenHost == "ch_a" ? _fx.ProxyAName : _fx.ProxyBName;

        await _fx.Client.AddToxicAsync(brokenProxy, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });

        try
        {
            // First post-chaos read: must succeed by failing over to the other node.
            var hostAfterFailover = await conn.ExecuteScalarAsync<string>("SELECT hostName()");
            Assert.Equal(survivingHost, hostAfterFailover);

            // Stickiness: subsequent reads should keep landing on the surviving
            // node, not flap back to the broken one.
            for (int i = 0; i < 5; i++)
            {
                var host = await conn.ExecuteScalarAsync<string>("SELECT hostName()");
                Assert.Equal(survivingHost, host);
            }
        }
        finally
        {
            await _fx.Client.RemoveAllToxicsAsync(brokenProxy);
        }
    }

    [Fact]
    public async Task Retry_AttemptsMultipleTimesBeforeGivingUp()
    {
        // Hard chaos that resets every connection. Asserts the retry policy fires the
        // configured number of attempts (visible in the AggregateException's inner list)
        // rather than failing on the first error. Recovery semantics on a connection-
        // level reset are a known gap and not asserted here.
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA },
            b => b.WithResilience(r => r.WithRetry(new RetryOptions
            {
                MaxRetries = 3,
                BaseDelay = TimeSpan.FromMilliseconds(50),
            })));

        await using var conn = new ResilientConnection(settings);
        await conn.OpenAsync();

        await _fx.Client.AddToxicAsync(_fx.ProxyAName, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });
        try
        {
            var ex = await Assert.ThrowsAnyAsync<Exception>(() =>
                conn.ExecuteScalarAsync<int>("SELECT 7"));

            // Drill into AggregateException if present.
            var attempts = ex is AggregateException agg ? agg.InnerExceptions.Count : 1;
            Assert.True(attempts >= 2,
                $"Expected ≥ 2 retry attempts, observed {attempts}.");
        }
        finally
        {
            await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);
        }
    }

    [Fact]
    public async Task CircuitBreaker_OpensFastWhenAllEndpointsKeepFailing()
    {
        // Wire OTel meter pipeline so we can verify the CB actually transitioned state.
        var metrics = new List<Metric>();
        using var meter = Sdk.CreateMeterProviderBuilder()
            .AddMeter("CH.Native")
            .AddInMemoryExporter(metrics)
            .Build();

        // Both endpoints reset every connection; circuit breaker should open and start
        // failing fast (no further network attempts).
        await _fx.Client.AddToxicAsync(_fx.ProxyAName, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });
        await _fx.Client.AddToxicAsync(_fx.ProxyBName, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });

        try
        {
            var settings = _fx.BuildSettings(
                new[] { _fx.EndpointA, _fx.EndpointB },
                b => b.WithResilience(r => r
                    .WithRetry(new RetryOptions { MaxRetries = 1, BaseDelay = TimeSpan.FromMilliseconds(20) })
                    .WithCircuitBreaker(new CircuitBreakerOptions
                    {
                        FailureThreshold = 3,
                        OpenDuration = TimeSpan.FromSeconds(2),
                    })));

            await using var conn = new ResilientConnection(settings);

            // Burn the threshold.
            for (int i = 0; i < 6; i++)
            {
                try { await conn.ExecuteScalarAsync<int>("SELECT 1"); } catch { }
            }

            // After tripping, subsequent calls should fail fast — well under BaseDelay
            // (20ms × small backoff). 500ms is a strict ceiling; was 1500ms.
            var sw = Stopwatch.StartNew();
            try { await conn.ExecuteScalarAsync<int>("SELECT 1"); } catch { }
            sw.Stop();

            Assert.True(sw.ElapsedMilliseconds < 500,
                $"After CB trip, call should fail fast (< 500ms); took {sw.ElapsedMilliseconds}ms");

            // Verify the breaker actually transitioned — the state-change counter
            // should have ticked at least once (closed → open).
            meter!.ForceFlush(timeoutMilliseconds: 1000);
            long stateChanges = 0;
            foreach (var m in metrics.Where(x => x.Name == "ch_native_circuit_breaker_state_changes"))
                foreach (var p in m.GetMetricPoints())
                    stateChanges += p.GetSumLong();
            Assert.True(stateChanges >= 1,
                $"Expected ≥ 1 circuit-breaker state change; saw {stateChanges}.");
        }
        finally
        {
            await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);
            await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyBName);
        }
    }
}
