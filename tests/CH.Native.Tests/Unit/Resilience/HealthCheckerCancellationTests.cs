using CH.Native.Connection;
using CH.Native.Resilience;
using Xunit;

namespace CH.Native.Tests.Unit.Resilience;

/// <summary>
/// Confirms that <see cref="HealthChecker"/> fully stops its background loop when
/// <see cref="HealthChecker.DisposeAsync"/> is called. Finding #11 suggested that
/// the background task was spawned with a different (or no) cancellation token and
/// would therefore outlive disposal.
/// </summary>
public class HealthCheckerCancellationTests
{
    private static ClickHouseConnectionSettings UnreachableSettings() =>
        new ClickHouseConnectionSettingsBuilder()
            // Port-0 with a short connect-timeout ensures checks fail fast and the
            // background loop spins quickly.
            .WithHost("127.0.0.1")
            .WithPort(1)
            .WithUsername("default")
            .WithPassword("")
            .WithConnectTimeout(TimeSpan.FromMilliseconds(250))
            .Build();

    [Fact]
    public async Task DisposeAsync_StopsBackgroundLoop_WithinSeveralChecks()
    {
        var servers = new[] { new ServerAddress("127.0.0.1", 1) };

        var checker = new HealthChecker(
            servers,
            UnreachableSettings(),
            checkInterval: TimeSpan.FromMilliseconds(50),
            healthCheckTimeout: TimeSpan.FromMilliseconds(250));

        int checksCompleted = 0;
        checker.OnHealthCheckCompleted += (_, __) => Interlocked.Increment(ref checksCompleted);

        // Wait for at least one background check to fire so we know the loop is alive.
        for (int i = 0; i < 100 && Volatile.Read(ref checksCompleted) == 0; i++)
            await Task.Delay(50);

        Assert.True(Volatile.Read(ref checksCompleted) > 0, "Background loop did not fire any checks");

        // Dispose should return promptly and stop further checks.
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await checker.DisposeAsync();
        stopwatch.Stop();

        // Disposal time should be bounded by a single in-flight health-check timeout.
        // Allow a healthy margin for CI load.
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5),
            $"DisposeAsync took {stopwatch.Elapsed} — should be bounded by the health-check timeout.");

        // After a brief settle, the check count should stop growing.
        var snapshot = Volatile.Read(ref checksCompleted);
        await Task.Delay(500);
        Assert.Equal(snapshot, Volatile.Read(ref checksCompleted));
    }

    [Fact]
    public async Task CheckAllAsync_CallerToken_IsIndependentOfBackgroundLoop()
    {
        // Ensures that calling the public CheckAllAsync with an already-canceled token
        // does NOT affect the background loop, and vice versa. Confirms that the two
        // tokens are independent.
        var servers = new[] { new ServerAddress("127.0.0.1", 1) };

        await using var checker = new HealthChecker(
            servers,
            UnreachableSettings(),
            checkInterval: TimeSpan.FromMilliseconds(50),
            healthCheckTimeout: TimeSpan.FromMilliseconds(250));

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Caller-token cancellation bubbles up but leaves the instance usable.
        var ex = await Record.ExceptionAsync(() => checker.CheckAllAsync(cts.Token));
        Assert.True(ex is null or OperationCanceledException);

        // Background loop should still be firing on its own token. Wait for at least
        // one check to happen after the caller-token cancellation.
        int checks = 0;
        checker.OnHealthCheckCompleted += (_, __) => Interlocked.Increment(ref checks);

        for (int i = 0; i < 100 && Volatile.Read(ref checks) == 0; i++)
            await Task.Delay(50);

        Assert.True(Volatile.Read(ref checks) > 0,
            "Cancelling a caller-token should not stop the background loop.");
    }
}
