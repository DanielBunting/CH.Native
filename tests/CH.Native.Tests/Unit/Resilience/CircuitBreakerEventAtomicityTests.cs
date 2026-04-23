using System.Collections.Concurrent;
using CH.Native.Resilience;
using Xunit;

namespace CH.Native.Tests.Unit.Resilience;

/// <summary>
/// Finding #12: under concurrent failures while the circuit is HalfOpen, more than one
/// thread can pass the HalfOpen state check and record a failure. The first thread's
/// HalfOpen→Open transition raises an event; subsequent threads then fall through into
/// the Closed branch (because state has already flipped to Open) and may emit Open→Open
/// same-state events. These tests characterize the current behavior.
/// </summary>
public class CircuitBreakerEventAtomicityTests
{
    [Fact]
    public async Task ConcurrentFailureInHalfOpen_RaisesExpectedHalfOpenToOpenEventOnce()
    {
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMilliseconds(50),
        };
        var breaker = new CircuitBreaker(options);

        // Drive it to HalfOpen: one failure, wait out the open duration.
        breaker.RecordFailure();
        Assert.Equal(CircuitBreakerState.Open, breaker.State);

        for (int i = 0; i < 50 && breaker.State != CircuitBreakerState.HalfOpen; i++)
            await Task.Delay(20);

        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);

        var events = new ConcurrentBag<CircuitBreakerStateChangedEventArgs>();
        breaker.OnStateChanged += (_, args) => events.Add(args);

        // Fan out concurrent failures. The HalfOpen→Open transition should logically
        // fire exactly once.
        var barrier = new Barrier(16);
        var tasks = Enumerable.Range(0, 16).Select(_ => Task.Run(() =>
        {
            barrier.SignalAndWait();
            breaker.RecordFailure();
        })).ToArray();
        await Task.WhenAll(tasks);

        // Let the async event dispatch (Task.Run) complete.
        for (int i = 0; i < 100; i++)
            await Task.Delay(20);

        var eventList = events.ToList();

        // The meaningful HalfOpen→Open transition should appear exactly once.
        var halfOpenToOpen = eventList.Count(e =>
            e.OldState == CircuitBreakerState.HalfOpen && e.NewState == CircuitBreakerState.Open);
        Assert.Equal(1, halfOpenToOpen);

        // Document whether any Open→Open (same-state) events slipped through. This is
        // the duplicate-event symptom from finding #12. A non-zero count is the bug.
        var openToOpen = eventList.Count(e =>
            e.OldState == CircuitBreakerState.Open && e.NewState == CircuitBreakerState.Open);

        // If the implementation is fixed to CAS only the winning transition, this
        // should stay zero.
        Assert.Equal(0, openToOpen);
    }

    [Fact]
    public async Task ConcurrentFailureInHalfOpen_DoesNotDoubleRaiseLogicalTransition()
    {
        // Alternative assertion: the total number of events from any Open↔HalfOpen
        // transitions should equal the number of logical transitions (1 in this test).
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMilliseconds(30),
        };
        var breaker = new CircuitBreaker(options);

        breaker.RecordFailure();
        for (int i = 0; i < 50 && breaker.State != CircuitBreakerState.HalfOpen; i++)
            await Task.Delay(20);
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);

        var events = new ConcurrentBag<CircuitBreakerStateChangedEventArgs>();
        breaker.OnStateChanged += (_, args) => events.Add(args);

        Parallel.For(0, 32, _ => breaker.RecordFailure());

        for (int i = 0; i < 100; i++)
            await Task.Delay(20);

        // Exactly one logical HalfOpen→Open state change should have been observed.
        Assert.Single(events);
    }
}
