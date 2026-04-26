using CH.Native.Connection;
using CH.Native.Resilience;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Verifies that <see cref="ResilientConnection"/> recovers between retries when
/// the underlying TCP connection is poisoned by a peer-side reset.
///
/// <para>History</para>
/// Originally this file pinned a *gap*: each retry attempt wrote to the same
/// broken socket because <see cref="ResilientConnection.ExecuteWithResilienceAsync"/>
/// only closed the connection in its outer catch (after all retries were
/// exhausted), not between attempts. The fix added a per-attempt eviction guarded
/// by <see cref="RetryPolicy.IsConnectionPoisoning"/>; these tests now assert
/// recovery rather than document the gap.
/// </summary>
[Collection("MultiToxiproxy")]
[Trait(Categories.Name, Categories.Resilience)]
public class RetryReconnectGapTests
{
    private readonly MultiToxiproxyFixture _fx;
    private readonly ITestOutputHelper _output;

    public RetryReconnectGapTests(MultiToxiproxyFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task TransientReset_ClearedDuringBackoff_RetrySucceeds()
    {
        // Inject a peer reset, then clear it ~300 ms later — well within the
        // retry budget. Pre-fix this hung in retry exhaustion; post-fix the
        // wrapper in ExecuteWithResilienceAsync evicts the poisoned connection
        // between attempts so the next try opens a fresh socket and succeeds.
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA },
            b => b.WithResilience(r => r.WithRetry(new RetryOptions
            {
                MaxRetries = 6,
                BaseDelay = TimeSpan.FromMilliseconds(150),
            })));

        await using var conn = new ResilientConnection(settings);
        await conn.OpenAsync();

        await _fx.Client.AddToxicAsync(_fx.ProxyAName, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });

        var clearer = Task.Run(async () =>
        {
            await Task.Delay(300);
            await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);
        });

        try
        {
            var v = await conn.ExecuteScalarAsync<int>("SELECT 7");
            Assert.Equal(7, v);
        }
        finally
        {
            await clearer;
            await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);
        }
    }

    [Fact]
    public async Task TransientReset_PersistentChaos_StillFailsAfterMaxRetries()
    {
        // Negative case to keep the retry-bounded contract honest: if chaos never
        // clears, we still terminate after MaxRetries+1 attempts and don't loop.
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA },
            b => b.WithResilience(r => r.WithRetry(new RetryOptions
            {
                MaxRetries = 2,
                BaseDelay = TimeSpan.FromMilliseconds(20),
            })));

        await using var conn = new ResilientConnection(settings);
        await conn.OpenAsync();

        await _fx.Client.AddToxicAsync(_fx.ProxyAName, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });
        try
        {
            var ex = await Assert.ThrowsAnyAsync<Exception>(() =>
                conn.ExecuteScalarAsync<int>("SELECT 1"));
            int attempts = ex is AggregateException agg ? agg.InnerExceptions.Count : 1;
            _output.WriteLine($"Persistent chaos — attempts before giving up: {attempts}");
            // MaxRetries=2, so total attempts should be exactly 1 initial + 2 retries = 3.
            Assert.Equal(3, attempts);
        }
        finally
        {
            await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);
        }
    }

    [Fact]
    public async Task ChaosClears_NextCallAfterFailedBatch_Works()
    {
        // Counterpart: once the chaos clears AND the previous failed call has
        // surfaced, the NEXT call from the user should succeed because the outer
        // catch block did close the connection. This pins the "recovery happens
        // between user calls, not between retries" semantics.
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA },
            b => b.WithResilience(r => r.WithRetry(new RetryOptions
            {
                MaxRetries = 2,
                BaseDelay = TimeSpan.FromMilliseconds(10),
            })));

        await using var conn = new ResilientConnection(settings);
        await conn.OpenAsync();

        await _fx.Client.AddToxicAsync(_fx.ProxyAName, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });
        try { await conn.ExecuteScalarAsync<int>("SELECT 1"); } catch { /* expected */ }

        await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);

        // After chaos clears, the next user call should reconnect and succeed.
        var v = await conn.ExecuteScalarAsync<int>("SELECT 99");
        Assert.Equal(99, v);
    }
}
