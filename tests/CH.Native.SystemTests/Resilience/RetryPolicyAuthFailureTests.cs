using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Resilience;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Permanent connection failures (wrong credentials, unknown user, unknown database)
/// must short-circuit the retry policy. Pre-fix the policy classified every
/// <see cref="ClickHouseConnectionException"/> as transient, so an auth failure
/// burned through the full retry budget before surfacing — wasting time and, in
/// some configurations, locking accounts.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class RetryPolicyAuthFailureTests
{
    private readonly SingleNodeFixture _fx;

    public RetryPolicyAuthFailureTests(SingleNodeFixture fx) => _fx = fx;

    [Fact]
    public async Task WrongPassword_DoesNotRetry()
    {
        // 5 retries × 500 ms base × 2x backoff ≥ 0.5 + 1 + 2 + 4 + 8 = 15.5 s of
        // backoff alone if the policy retries. The connect-attempt time itself adds
        // more. Auth rejection arrives during the first handshake, so once the fix
        // lands we should be back well under one second.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithCredentials(_fx.Username, "definitely-not-the-password")
            .WithResilience(r => r.WithRetry(new RetryOptions
            {
                MaxRetries = 5,
                BaseDelay = TimeSpan.FromMilliseconds(500),
                BackoffMultiplier = 2.0,
            }))
            .Build();

        await using var conn = new ResilientConnection(settings);

        var sw = Stopwatch.StartNew();
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => conn.OpenAsync());
        sw.Stop();

        // A retried failure produces an AggregateException with one inner per attempt.
        // A single attempt bubbles the original exception untouched.
        if (ex is AggregateException agg)
        {
            Assert.Fail($"Auth failure was retried — saw {agg.InnerExceptions.Count} inner exceptions.");
        }

        Assert.True(
            sw.Elapsed < TimeSpan.FromSeconds(3),
            $"Auth failure took {sw.Elapsed.TotalSeconds:F2}s; budget would be ~15s if retried.");

        // The exception should be typed so callers can distinguish auth failures.
        Assert.IsAssignableFrom<ClickHouseAuthenticationException>(ex);
    }

    [Fact]
    public async Task UnknownUser_DoesNotRetry()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithCredentials("user_that_does_not_exist", "anything")
            .WithResilience(r => r.WithRetry(new RetryOptions
            {
                MaxRetries = 5,
                BaseDelay = TimeSpan.FromMilliseconds(500),
                BackoffMultiplier = 2.0,
            }))
            .Build();

        await using var conn = new ResilientConnection(settings);

        var sw = Stopwatch.StartNew();
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => conn.OpenAsync());
        sw.Stop();

        if (ex is AggregateException agg)
        {
            Assert.Fail($"Unknown-user failure was retried — saw {agg.InnerExceptions.Count} inner exceptions.");
        }

        Assert.True(
            sw.Elapsed < TimeSpan.FromSeconds(3),
            $"Unknown-user failure took {sw.Elapsed.TotalSeconds:F2}s; budget would be ~15s if retried.");

        Assert.IsAssignableFrom<ClickHouseAuthenticationException>(ex);
    }

    [Fact]
    public void IsTransientException_AuthenticationFailure_ReturnsFalse()
    {
        // White-box: the predicate itself must reject auth failures even when wrapped
        // as ClickHouseConnectionException (the existing handshake path) so future
        // call sites are protected too.
        var ex = new ClickHouseAuthenticationException("Authentication failed");
        Assert.False(RetryPolicy.IsTransientException(ex));
        Assert.False(RetryPolicy.IsConnectionPoisoning(ex));
    }
}
