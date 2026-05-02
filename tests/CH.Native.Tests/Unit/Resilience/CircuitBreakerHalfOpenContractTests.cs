using CH.Native.Exceptions;
using CH.Native.Resilience;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace CH.Native.Tests.Unit.Resilience;

/// <summary>
/// Lock-in tests for the circuit-breaker half-open probe semantics.
///
/// <para>
/// Existing <see cref="CircuitBreakerTests"/> covers the basic state machine
/// (Closed→Open after threshold; Open→HalfOpen after duration; HalfOpen+success→
/// Closed; HalfOpen+failure→Open). This file pins the <em>finer</em> half-open
/// contract that's easy to break in a refactor:
/// </para>
/// <list type="number">
/// <item><description>A single success closes — there is no N-successful-probe requirement.</description></item>
/// <item><description>A failure in HalfOpen restarts the OpenDuration timer.</description></item>
/// <item><description>While in HalfOpen, <c>AllowRequest</c> returns true (probe allowed).</description></item>
/// <item><description>HalfOpen accepts <em>concurrent</em> probes — the breaker does not
///     gate to a single probe (this would mean a forensic regression if added).</description></item>
/// <item><description>The State getter, when it auto-transitions Open→HalfOpen, fires
///     <c>OnStateChanged</c> exactly once.</description></item>
/// <item><description>Cancellation during a HalfOpen probe is not counted as a failure
///     (matches Closed-state behavior in <see cref="CircuitBreaker.ExecuteAsync"/>).</description></item>
/// </list>
/// </summary>
[Collection("MeterTests")]
public class CircuitBreakerHalfOpenContractTests
{
    private static (CircuitBreaker breaker, FakeTimeProvider time) NewBreaker(
        int failureThreshold = 1,
        TimeSpan? openDuration = null)
    {
        var time = new FakeTimeProvider();
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = failureThreshold,
            OpenDuration = openDuration ?? TimeSpan.FromSeconds(30),
            FailureWindow = TimeSpan.FromMinutes(1),
        };
        return (new CircuitBreaker(options, logger: null, timeProvider: time), time);
    }

    private static async Task TripOpenAsync(CircuitBreaker breaker)
    {
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await breaker.ExecuteAsync<int>(async _ =>
            {
                await Task.Yield();
                throw new InvalidOperationException("trip");
            });
        });
    }

    [Fact]
    public async Task SingleSuccessProbe_ClosesBreaker_NoNRequirement()
    {
        // Pins: there is exactly ONE successful probe required to close.
        // If a future change adds an N-probe rule, this test flips and the
        // contract change is visible in code review.
        var (breaker, time) = NewBreaker();
        await TripOpenAsync(breaker);
        time.Advance(TimeSpan.FromSeconds(31));

        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);

        var result = await breaker.ExecuteAsync(async _ => { await Task.Yield(); return 42; });

        Assert.Equal(42, result);
        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
    }

    [Fact]
    public async Task FailureInHalfOpen_RestartsOpenDurationTimer()
    {
        // Lock-in: when HalfOpen→Open after a failed probe, the OpenDuration
        // is measured from the moment of re-open, not from the original open.
        // _lastStateChange is reassigned in OnFailure (CircuitBreaker.cs lines 380-381).
        // A regression that forgot to reset _lastStateChange would let the
        // very next State read transition straight back to HalfOpen.
        var (breaker, time) = NewBreaker(openDuration: TimeSpan.FromSeconds(30));
        await TripOpenAsync(breaker);

        time.Advance(TimeSpan.FromSeconds(31));
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);

        // Probe fails → re-open. The timer must restart from now.
        await TripOpenAsync(breaker);
        Assert.Equal(CircuitBreakerState.Open, breaker.State);

        // Advance 29s — still inside the new OpenDuration window.
        time.Advance(TimeSpan.FromSeconds(29));
        Assert.Equal(CircuitBreakerState.Open, breaker.State);

        // Cross 30s from re-open — only now should it transition to HalfOpen.
        time.Advance(TimeSpan.FromSeconds(2));
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);
    }

    [Fact]
    public async Task AllowRequest_InHalfOpen_ReturnsTrue()
    {
        var (breaker, time) = NewBreaker();
        await TripOpenAsync(breaker);
        time.Advance(TimeSpan.FromSeconds(31));

        // Reading State performs the auto-transition; AllowRequest also does.
        // Use AllowRequest to exercise it directly.
        Assert.True(breaker.AllowRequest());
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);
    }

    [Fact]
    public async Task ConcurrentHalfOpenProbes_BothExecute()
    {
        // Pins: HalfOpen does NOT gate to a single in-flight probe. Both
        // concurrent callers reach the action; the first to complete with
        // a result drives the state. If a future refactor introduces a
        // single-probe gate, this assertion will fail and the design change
        // becomes visible.
        var (breaker, time) = NewBreaker();
        await TripOpenAsync(breaker);
        time.Advance(TimeSpan.FromSeconds(31));
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);

        int probeEntered = 0;
        var startGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        async Task<int> Probe()
        {
            return await breaker.ExecuteAsync(async _ =>
            {
                Interlocked.Increment(ref probeEntered);
                await startGate.Task; // both probes wait here, proving concurrency
                return 1;
            });
        }

        var probe1 = Probe();
        var probe2 = Probe();

        // Wait for both probes to be parked inside the breaker action.
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (Volatile.Read(ref probeEntered) < 2 && DateTime.UtcNow < deadline)
            await Task.Yield();

        Assert.Equal(2, Volatile.Read(ref probeEntered));

        startGate.SetResult(true);
        await probe1;
        await probe2;

        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
    }

    [Fact]
    public async Task StateGetter_AutoTransition_FiresOnStateChangedExactlyOnce()
    {
        // Reading State after OpenDuration must fire OnStateChanged exactly
        // once for Open→HalfOpen. A naive implementation that re-checks each
        // call could fire repeatedly.
        var (breaker, time) = NewBreaker();
        await TripOpenAsync(breaker);
        time.Advance(TimeSpan.FromSeconds(31));

        int stateChangedCount = 0;
        breaker.OnStateChanged += (_, args) =>
        {
            if (args.OldState == CircuitBreakerState.Open
                && args.NewState == CircuitBreakerState.HalfOpen)
            {
                Interlocked.Increment(ref stateChangedCount);
            }
        };

        // Read State multiple times — the first returns HalfOpen and fires
        // exactly one transition event; subsequent reads return HalfOpen
        // without re-firing.
        _ = breaker.State;
        _ = breaker.State;
        _ = breaker.AllowRequest();

        Assert.Equal(1, Volatile.Read(ref stateChangedCount));
    }

    [Fact]
    public async Task CancellationDuringHalfOpenProbe_DoesNotReopen()
    {
        // Cancellation must not be counted as a probe failure — same rule
        // as Closed state (CircuitBreaker.cs lines 155-159 short-circuits
        // OperationCanceledException).
        var (breaker, time) = NewBreaker();
        await TripOpenAsync(breaker);
        time.Advance(TimeSpan.FromSeconds(31));
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await breaker.ExecuteAsync<int>(async ct =>
            {
                ct.ThrowIfCancellationRequested();
                await Task.Yield();
                return 0;
            }, cts.Token);
        });

        // Should remain HalfOpen — cancellation didn't reopen, didn't close.
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);
    }

    [Fact]
    public async Task RecordSuccessFromHalfOpen_ClosesBreaker()
    {
        // The non-ExecuteAsync path: callers that drive the breaker manually
        // via RecordSuccess/RecordFailure get the same HalfOpen→Closed
        // single-success transition.
        var (breaker, time) = NewBreaker();
        await TripOpenAsync(breaker);
        time.Advance(TimeSpan.FromSeconds(31));
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);

        breaker.RecordSuccess();
        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
    }

    [Fact]
    public async Task RecordFailureFromHalfOpen_ReopensBreaker_AndRestartsTimer()
    {
        var (breaker, time) = NewBreaker(openDuration: TimeSpan.FromSeconds(30));
        await TripOpenAsync(breaker);
        time.Advance(TimeSpan.FromSeconds(31));
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);

        breaker.RecordFailure();
        Assert.Equal(CircuitBreakerState.Open, breaker.State);

        time.Advance(TimeSpan.FromSeconds(15));
        Assert.Equal(CircuitBreakerState.Open, breaker.State);

        time.Advance(TimeSpan.FromSeconds(16));
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);
    }

    [Fact]
    public async Task ProbeRejected_OnceOpenDurationNotElapsed()
    {
        // Sanity: while still Open (timer not elapsed), ExecuteAsync throws
        // CircuitBreakerOpenException without invoking the action.
        var (breaker, time) = NewBreaker(openDuration: TimeSpan.FromSeconds(30));
        await TripOpenAsync(breaker);

        time.Advance(TimeSpan.FromSeconds(10));

        bool actionInvoked = false;
        await Assert.ThrowsAsync<CircuitBreakerOpenException>(async () =>
        {
            await breaker.ExecuteAsync(async _ =>
            {
                actionInvoked = true;
                await Task.Yield();
                return 0;
            });
        });

        Assert.False(actionInvoked, "action must not be invoked while breaker is Open");
    }
}
