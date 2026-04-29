using CH.Native.Resilience;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pins the steady-state distribution behaviour of <see cref="LoadBalancingStrategy"/>.
/// Existing chaos tests prove failover; nothing today proves the strategies actually
/// distribute requests across multiple healthy nodes during normal operation. A
/// regression that pinned all traffic to one node would currently pass.
///
/// <para>
/// The LB rotates on (re)connect, not per-query. To exercise rotation, each iteration
/// calls <see cref="ResilientConnection.CloseAsync"/> before the next operation —
/// forcing <c>EnsureConnectedAsync</c> to consult <c>LoadBalancer.GetNextServer()</c>
/// again. Server-side <c>SELECT hostName()</c> is the ground truth (matches one of
/// the two container hostnames, "ch_a" / "ch_b").
/// </para>
/// </summary>
[Collection("MultiToxiproxy")]
[Trait(Categories.Name, Categories.Resilience)]
public class LoadBalancerDistributionTests
{
    private readonly MultiToxiproxyFixture _fx;
    private readonly ITestOutputHelper _output;

    public LoadBalancerDistributionTests(MultiToxiproxyFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task RoundRobin_DistributesEvenlyAcrossEndpoints()
    {
        const int iterations = 300;
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA, _fx.EndpointB },
            b => b.WithLoadBalancing(LoadBalancingStrategy.RoundRobin));

        await using var conn = new ResilientConnection(settings);
        await conn.OpenAsync();

        var counts = new Dictionary<string, int> { ["ch_a"] = 0, ["ch_b"] = 0 };
        for (int i = 0; i < iterations; i++)
        {
            await conn.CloseAsync();
            var host = await conn.ExecuteScalarAsync<string>("SELECT hostName()");
            Assert.NotNull(host);
            Assert.True(counts.ContainsKey(host!), $"Unexpected host: {host}");
            counts[host!]++;
        }

        _output.WriteLine($"RoundRobin distribution: ch_a={counts["ch_a"]}, ch_b={counts["ch_b"]}");

        // Round-robin guarantees ±1 of perfect 50/50 split — the only allowed slop is
        // the parity of the start index. ±5% would mask a regression that picked the
        // same node twice in a row; tighten to ±3.
        Assert.InRange(counts["ch_a"], iterations / 2 - 3, iterations / 2 + 3);
        Assert.InRange(counts["ch_b"], iterations / 2 - 3, iterations / 2 + 3);
    }

    [Fact]
    public async Task Random_DistributionWithinChiSquaredBound()
    {
        const int iterations = 600;
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA, _fx.EndpointB },
            b => b.WithLoadBalancing(LoadBalancingStrategy.Random));

        await using var conn = new ResilientConnection(settings);
        await conn.OpenAsync();

        int a = 0, b = 0;
        for (int i = 0; i < iterations; i++)
        {
            await conn.CloseAsync();
            var host = await conn.ExecuteScalarAsync<string>("SELECT hostName()");
            if (host == "ch_a") a++; else if (host == "ch_b") b++;
        }

        _output.WriteLine($"Random distribution: ch_a={a}, ch_b={b}");

        Assert.Equal(iterations, a + b);

        // Two equal-probability buckets, df = 1. χ² = Σ((O-E)²/E). The 99% critical
        // value is 6.635 — set the bound there so the test is robust to seed noise
        // but still catches the regression "Random always returns 0".
        double expected = iterations / 2.0;
        double chiSquared = Math.Pow(a - expected, 2) / expected
                          + Math.Pow(b - expected, 2) / expected;
        _output.WriteLine($"χ² = {chiSquared:F3} (threshold 6.635)");
        Assert.True(chiSquared < 6.635,
            $"Random distribution failed χ² test: {chiSquared:F3} ≥ 6.635 (a={a}, b={b}).");
    }

    [Fact]
    public async Task FirstAvailable_AlwaysPicksFirstWhileHealthy()
    {
        const int iterations = 100;
        // Endpoint A first in the list — FirstAvailable must always pick it while
        // it's healthy (no chaos in this test).
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA, _fx.EndpointB },
            b => b.WithLoadBalancing(LoadBalancingStrategy.FirstAvailable));

        await using var conn = new ResilientConnection(settings);
        await conn.OpenAsync();

        // Identify which container A maps to — depends on which proxy points where.
        await conn.CloseAsync();
        var firstHost = await conn.ExecuteScalarAsync<string>("SELECT hostName()");
        Assert.NotNull(firstHost);

        for (int i = 0; i < iterations - 1; i++)
        {
            await conn.CloseAsync();
            var host = await conn.ExecuteScalarAsync<string>("SELECT hostName()");
            Assert.Equal(firstHost, host);
        }
    }
}
