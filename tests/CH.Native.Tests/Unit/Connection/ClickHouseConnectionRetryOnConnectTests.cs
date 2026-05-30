using CH.Native.Connection;
using CH.Native.Resilience;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

/// <summary>
/// Covers the retry-on-connect path wired into <see cref="ClickHouseConnection.OpenAsync"/>:
/// when <c>settings.Resilience.Retry</c> is configured, connect+handshake run
/// inside the retry policy and each failed attempt resets socket/pipe state
/// before the next. Exhaustion is exercised deterministically against a refused
/// port; the policy's <see cref="RetryOptions.ShouldRetry"/> probe lets the test
/// confirm retries actually happened without needing a flaky server.
/// </summary>
public class ClickHouseConnectionRetryOnConnectTests
{
    [Fact]
    public async Task OpenAsync_WithRetry_RetriesTransientConnectFailures_ThenThrows()
    {
        var attempts = 0;
        var retry = new RetryOptions
        {
            MaxRetries = 2,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            MaxDelay = TimeSpan.FromMilliseconds(5),
            // Count every classification call and force-retry, so the assertion
            // is independent of how the OS surfaces a refused connection.
            ShouldRetry = _ => { Interlocked.Increment(ref attempts); return true; },
        };

        var settings = new ClickHouseConnectionSettingsBuilder()
            .WithHost("127.0.0.1")
            .WithPort(1) // tcpmux — effectively never listening; connect is refused fast
            .WithConnectTimeout(TimeSpan.FromSeconds(2))
            .WithResilience(r => r.WithRetry(retry))
            .Build();

        await using var conn = new ClickHouseConnection(settings);

        // All attempts fail → OpenAsync surfaces the underlying connect failure.
        await Assert.ThrowsAnyAsync<Exception>(() => conn.OpenAsync());

        // MaxRetries=2 means the policy consults ShouldRetry on the first two
        // failures (then gives up on the third), proving the retry branch +
        // per-attempt CloseInternalAsync reset ran.
        Assert.Equal(2, Volatile.Read(ref attempts));
    }
}
