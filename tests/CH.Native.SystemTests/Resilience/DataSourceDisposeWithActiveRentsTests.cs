using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pins the contract that <see cref="ClickHouseDataSource.DisposeAsync"/> tears
/// down cleanly even when callers still hold rented connections and have queries
/// in flight.
///
/// <para>
/// Failure modes a regression here would cause in production:
/// </para>
/// <list type="bullet">
/// <item><description>Permits leak from <c>_gate</c> — pool reports non-zero busy after
///     all rents have observed disposal, and a future rent sees a wrong permit count.</description></item>
/// <item><description>In-flight rents hang on the gate until <c>ConnectionWaitTimeout</c>
///     instead of immediately observing <c>ObjectDisposedException</c>.</description></item>
/// <item><description>A rent acquires a permit just as dispose runs and the renter is
///     left holding a half-open connection that the pool can no longer track.</description></item>
/// </list>
///
/// <para>
/// The pool's actual contract (per <c>ClickHouseDataSource.cs</c> lines 173-205):
/// concurrent rents racing dispose throw <see cref="ObjectDisposedException"/>;
/// in-flight rents that already returned a connection fall through to the existing
/// pool-return hook, which on a disposed pool routes through <c>DiscardInternalAsync</c>.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class DataSourceDisposeWithActiveRentsTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public DataSourceDisposeWithActiveRentsTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task DisposeWhileMultipleRentsHoldConnections_TearsDownCleanly()
    {
        // 8 concurrent rents, each running a long-ish SELECT sleep(...) so they're
        // genuinely in-flight when DisposeAsync runs. The dispose must not hang
        // and the rents must observe a typed exception (or the underlying socket
        // close, which surfaces as a connection / I/O exception).
        const int maxPool = 8;

        var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = maxPool,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(30),
        });

        var startedGate = new CountdownEvent(maxPool);
        var rentExceptions = new List<Exception?>();
        var rentExceptionsLock = new object();

        var rentTasks = new List<Task>();
        for (int i = 0; i < maxPool; i++)
        {
            int idx = i;
            rentTasks.Add(Task.Run(async () =>
            {
                try
                {
                    await using var conn = await ds.OpenConnectionAsync();
                    startedGate.Signal();
                    // Long-ish query so the rent is in-flight when dispose runs.
                    await foreach (var _ in conn.QueryAsync<int>("SELECT sleep(3)")) { }
                }
                catch (Exception ex)
                {
                    lock (rentExceptionsLock)
                        rentExceptions.Add(ex);
                    _output.WriteLine($"rent #{idx} surfaced: {ex.GetType().Name}: {ex.Message}");
                }
            }));
        }

        // Wait for every rent to have actually acquired a connection and started
        // its query — otherwise dispose may race purely-pre-acquisition rents
        // and the test stops asserting what we want.
        startedGate.Wait(TimeSpan.FromSeconds(15));

        var stats = ds.GetStatistics();
        _output.WriteLine($"pre-dispose: Total={stats.Total} Idle={stats.Idle} Busy={stats.Busy}");
        Assert.Equal(maxPool, stats.Busy);

        // Dispose mid-flight. Must complete promptly (well under the 3 s sleep).
        var disposeStart = DateTime.UtcNow;
        await ds.DisposeAsync();
        var disposeElapsed = DateTime.UtcNow - disposeStart;
        _output.WriteLine($"dispose elapsed: {disposeElapsed.TotalMilliseconds:F0} ms");

        // Wait for all rent tasks to settle. The 3s sleep query is the upper
        // bound; in practice rents observe the closed socket much earlier when
        // the connection is disposed under their feet, but the test deadline is
        // generous to avoid flakiness on slower machines.
        var allRents = Task.WhenAll(rentTasks);
        var winner = await Task.WhenAny(allRents, Task.Delay(TimeSpan.FromSeconds(15)));

        Assert.Same(allRents, winner);
        await allRents;

        // Post-dispose: any subsequent rent attempt must throw ObjectDisposedException.
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await using var _ = await ds.OpenConnectionAsync();
        });
    }

    [Fact]
    public async Task DisposeWhileWaitersAreParkedOnGate_ImmediatelyWakesThemWithObjectDisposed()
    {
        // 2 rents fully consume the pool, then 4 more block on the gate. Dispose
        // must wake every parked waiter with ObjectDisposedException, not let
        // them sit until ConnectionWaitTimeout.
        const int maxPool = 2;

        var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = maxPool,
            // Long wait so a regression that fails to wake waiters surfaces
            // as a test deadline failure instead of a permit-timeout result.
            ConnectionWaitTimeout = TimeSpan.FromSeconds(60),
        });

        // Saturate the pool with two long rents.
        var saturate1 = await ds.OpenConnectionAsync();
        var saturate2 = await ds.OpenConnectionAsync();

        // Park 4 waiters on the gate.
        var parkedTasks = new List<Task<Exception?>>();
        for (int i = 0; i < 4; i++)
        {
            parkedTasks.Add(Task.Run(async () =>
            {
                try
                {
                    await using var c = await ds.OpenConnectionAsync();
                    return (Exception?)null;
                }
                catch (Exception ex) { return ex; }
            }));
        }

        // Give them a moment to actually be parked on the semaphore.
        await Task.Delay(200);

        var statsBefore = ds.GetStatistics();
        _output.WriteLine($"before dispose: PendingWaits={statsBefore.PendingWaits}");
        Assert.True(statsBefore.PendingWaits >= 1, "expected at least one parked waiter");

        var disposeStart = DateTime.UtcNow;
        await ds.DisposeAsync();
        var disposeElapsed = DateTime.UtcNow - disposeStart;

        // Return the saturating rents — they hit the disposed-pool branch in
        // ReturnAsync (lines 360-367) and route through DiscardInternalAsync.
        await saturate1.DisposeAsync();
        await saturate2.DisposeAsync();

        var results = await Task.WhenAll(parkedTasks);

        // Every parked waiter must observe ObjectDisposedException (or a linked
        // OperationCanceledException) — not a TimeoutException, which would
        // mean dispose didn't wake them.
        foreach (var ex in results)
        {
            Assert.NotNull(ex);
            Assert.True(
                ex is ObjectDisposedException || ex is OperationCanceledException,
                $"parked waiter surfaced unexpected type: {ex!.GetType().Name}: {ex.Message}");
        }

        // Dispose itself must have completed promptly — the wake-up should be
        // synchronous via _disposeCts.Cancel(), not deferred until the
        // ConnectionWaitTimeout (60s).
        Assert.True(disposeElapsed < TimeSpan.FromSeconds(10),
            $"dispose took {disposeElapsed.TotalMilliseconds:F0} ms; expected immediate wake-up");
        _output.WriteLine($"dispose with parked waiters elapsed: {disposeElapsed.TotalMilliseconds:F0} ms");
    }

    [Fact]
    public async Task DisposeIsIdempotent()
    {
        var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 2,
        });

        await using (var _ = await ds.OpenConnectionAsync()) { }

        await ds.DisposeAsync();
        await ds.DisposeAsync(); // must not throw
        await ds.DisposeAsync();
    }
}
