using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.Resilience;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Chaos;

/// <summary>
/// Failure-injection tests using Toxiproxy. Each test resets toxics in its finally block
/// so test order does not matter.
/// </summary>
[Collection("Toxiproxy")]
[Trait(Categories.Name, Categories.Chaos)]
public class NetworkChaosTests : IAsyncLifetime
{
    private readonly ToxiproxyFixture _proxy;

    public NetworkChaosTests(ToxiproxyFixture proxy)
    {
        _proxy = proxy;
    }

    // Reset toxics before AND after each test. A failing assertion can short-circuit a
    // test before its finally block, leaving toxics in place that poison the next test.
    public Task InitializeAsync() => _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
    public Task DisposeAsync() => _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);

    [Fact]
    public async Task Latency_DownstreamSlowdown_QueriesStillSucceed()
    {
        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "latency", "downstream",
            new() { ["latency"] = 200, ["jitter"] = 20 });

        try
        {
            await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
            await conn.OpenAsync();

            var sw = Stopwatch.StartNew();
            var value = await conn.ExecuteScalarAsync<int>("SELECT 1");
            sw.Stop();

            Assert.Equal(1, value);
            // We added at least 200ms of one-way latency; round-trip should clearly exceed it.
            Assert.True(sw.ElapsedMilliseconds >= 150,
                $"Expected appreciable latency, observed {sw.ElapsedMilliseconds}ms");
        }
        finally
        {
            await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
        }
    }

    [Fact]
    public async Task HandshakeStall_CallerCanCancel()
    {
        // Bandwidth 0 upstream: TCP connect succeeds at the proxy, but no bytes ever
        // reach the server, so the CH handshake stalls indefinitely. The caller should
        // be able to abort via the cancellation token.
        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "bandwidth", "upstream",
            new() { ["rate"] = 0 });

        await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        var sw = Stopwatch.StartNew();
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => conn.OpenAsync(cts.Token));
        sw.Stop();

        // CT was 2s — allow up to 4s slack but no more (was 10s; far too generous).
        Assert.True(sw.ElapsedMilliseconds < 4_000,
            $"Handshake cancellation should have tripped near 2s, took {sw.ElapsedMilliseconds}ms");
        // Must surface as cancellation-typed (or wrapped) — not a generic Exception or NRE.
        var isCancellation = ex is OperationCanceledException
            || ex.InnerException is OperationCanceledException
            || RetryPolicy.IsConnectionPoisoning(ex);
        Assert.True(isCancellation,
            $"Expected OperationCanceledException or connection-typed exception, got {ex.GetType().FullName}: {ex.Message}");
    }

    [Fact]
    public async Task ResetPeer_MidQuery_SurfacesAsException()
    {
        await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
        await conn.OpenAsync();

        // Issue a long-running query so the connection is mid-flight when we reset.
        var queryTask = Task.Run(() => conn.ExecuteScalarAsync<ulong>(
            "SELECT count() FROM numbers(1000000000)"));

        await Task.Delay(150);

        // Inject reset_peer with very low timeout so the next packet kills the connection.
        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });

        try
        {
            // Must throw within a bounded window — no hang allowed.
            var sw = Stopwatch.StartNew();
            var ex = await Assert.ThrowsAnyAsync<Exception>(() => queryTask);
            sw.Stop();
            Assert.True(sw.ElapsedMilliseconds < 10_000,
                $"Mid-query reset should surface within 10s, took {sw.ElapsedMilliseconds}ms");

            // Must be classified as connection-poisoning so resilient callers reconnect.
            Assert.True(RetryPolicy.IsConnectionPoisoning(ex),
                $"Mid-query reset should be connection-poisoning; got {ex.GetType().FullName}: {ex.Message}");
        }
        finally
        {
            await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
        }
    }

    [Fact]
    public async Task BandwidthLimit_QuerySucceedsWithinReasonableTime()
    {
        // Throttle the downstream pipe to 64 KB/s. A small streamed result should still succeed.
        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "bandwidth", "downstream",
            new() { ["rate"] = 64 });

        try
        {
            await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
            await conn.OpenAsync();

            var sw = Stopwatch.StartNew();
            var rows = 0;
            await foreach (var row in conn.QueryAsync("SELECT number FROM numbers(1000)"))
            {
                _ = row.GetFieldValue<ulong>(0);
                rows++;
            }
            sw.Stop();
            Assert.Equal(1000, rows);
            // Hang guard: 1000 ulong rows at 64 KiB/s should complete in well under 60s.
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(60),
                $"Throttled query should complete within 60s, took {sw.Elapsed.TotalSeconds:F1}s");
        }
        finally
        {
            await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
        }
    }

    [Fact]
    public async Task LatencyUpstream_QueryStillSucceeds()
    {
        // 200ms upstream latency adds delay on the request path. Verifies the client
        // tolerates server-ack delay (today's tests only exercise downstream).
        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "latency", "upstream",
            new() { ["latency"] = 200, ["jitter"] = 20 });
        try
        {
            await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
            await conn.OpenAsync();
            var sw = Stopwatch.StartNew();
            var v = await conn.ExecuteScalarAsync<int>("SELECT 1");
            sw.Stop();
            Assert.Equal(1, v);
            Assert.True(sw.ElapsedMilliseconds >= 150,
                $"Expected ≥ 150ms upstream latency, observed {sw.ElapsedMilliseconds}ms");
        }
        finally
        {
            await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
        }
    }
}
