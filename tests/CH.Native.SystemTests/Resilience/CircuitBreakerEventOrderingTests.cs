using CH.Native.Resilience;
using Xunit;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pre-fix the breaker raised <c>OnStateChanged</c> via <c>Task.Run</c>, which
/// ran the handler on a thread-pool thread <em>after</em> the call site that
/// caused the transition had returned. By the time the handler ran, subsequent
/// calls could have moved the breaker into a new state, so the handler's
/// observed <c>sender.State</c> no longer matched its <c>args.NewState</c>.
///
/// The fix invokes handlers synchronously after the lock is released — caller
/// thread, no scheduling. These tests pin both: synchronous dispatch and
/// state-consistency at the moment of invocation.
/// </summary>
[Trait(Categories.Name, Categories.Resilience)]
public class CircuitBreakerEventOrderingTests
{
    [Fact]
    public void StateChanged_FiresSynchronouslyOnCallerThread()
    {
        var breaker = new CircuitBreaker(new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            FailureWindow = TimeSpan.FromSeconds(10),
            OpenDuration = TimeSpan.FromSeconds(10),
        });

        int handlerThread = -1;
        breaker.OnStateChanged += (_, _) => handlerThread = Environment.CurrentManagedThreadId;

        var callerThread = Environment.CurrentManagedThreadId;
        breaker.RecordFailure();

        Assert.Equal(callerThread, handlerThread);
    }

    [Fact]
    public void StateChanged_HandlerObservesNewState()
    {
        var breaker = new CircuitBreaker(new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            FailureWindow = TimeSpan.FromSeconds(10),
            OpenDuration = TimeSpan.FromSeconds(10),
        });

        CircuitBreakerState handlerObservedSenderState = default;
        CircuitBreakerState handlerObservedArgsState = default;
        breaker.OnStateChanged += (sender, args) =>
        {
            handlerObservedSenderState = ((CircuitBreaker)sender!).State;
            handlerObservedArgsState = args.NewState;
        };

        breaker.RecordFailure();

        Assert.Equal(CircuitBreakerState.Open, handlerObservedArgsState);
        Assert.Equal(handlerObservedArgsState, handlerObservedSenderState);
    }

    [Fact]
    public void OnReset_FiresSynchronously()
    {
        var breaker = new CircuitBreaker(new CircuitBreakerOptions
        {
            FailureThreshold = 1,
        });

        int handlerThread = -1;
        breaker.OnReset += (_, _) => handlerThread = Environment.CurrentManagedThreadId;

        breaker.RecordFailure();
        var callerThread = Environment.CurrentManagedThreadId;
        breaker.Reset();

        Assert.Equal(callerThread, handlerThread);
    }
}
