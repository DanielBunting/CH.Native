using CH.Native.Exceptions;
using CH.Native.Resilience;
using Xunit;

namespace CH.Native.Tests.Unit.Resilience;

public class CircuitBreakerTests
{
    [Fact]
    public void State_InitialState_IsClosed()
    {
        var breaker = new CircuitBreaker();
        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
    }

    [Fact]
    public async Task ExecuteAsync_Success_ReturnResult()
    {
        var breaker = new CircuitBreaker();

        var result = await breaker.ExecuteAsync(async _ =>
        {
            await Task.Yield();
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
    }

    [Fact]
    public async Task ExecuteAsync_FailuresBelowThreshold_StaysClosed()
    {
        var options = new CircuitBreakerOptions { FailureThreshold = 5 };
        var breaker = new CircuitBreaker(options);

        // 4 failures (below threshold of 5)
        for (var i = 0; i < 4; i++)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await breaker.ExecuteAsync<int>(async _ =>
                {
                    await Task.Yield();
                    throw new InvalidOperationException();
                });
            });
        }

        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        Assert.Equal(4, breaker.FailureCount);
    }

    [Fact]
    public async Task ExecuteAsync_FailuresReachThreshold_OpensCircuit()
    {
        var options = new CircuitBreakerOptions { FailureThreshold = 3 };
        var breaker = new CircuitBreaker(options);

        // 3 failures (reaches threshold)
        for (var i = 0; i < 3; i++)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await breaker.ExecuteAsync<int>(async _ =>
                {
                    await Task.Yield();
                    throw new InvalidOperationException();
                });
            });
        }

        Assert.Equal(CircuitBreakerState.Open, breaker.State);
    }

    [Fact]
    public async Task ExecuteAsync_WhenOpen_ThrowsCircuitBreakerOpenException()
    {
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMinutes(5)
        };
        var breaker = new CircuitBreaker(options);

        // Trigger open state
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await breaker.ExecuteAsync<int>(async _ =>
            {
                await Task.Yield();
                throw new InvalidOperationException();
            });
        });

        Assert.Equal(CircuitBreakerState.Open, breaker.State);

        // Next call should throw CircuitBreakerOpenException
        var ex = await Assert.ThrowsAsync<CircuitBreakerOpenException>(async () =>
        {
            await breaker.ExecuteAsync(async _ =>
            {
                await Task.Yield();
                return 42;
            });
        });

        Assert.True(ex.TimeUntilReset > TimeSpan.Zero);
    }

    [Fact]
    public async Task ExecuteAsync_OpenDurationExpires_TransitionsToHalfOpen()
    {
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMilliseconds(50)
        };
        var breaker = new CircuitBreaker(options);

        // Trigger open state
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await breaker.ExecuteAsync<int>(async _ =>
            {
                await Task.Yield();
                throw new InvalidOperationException();
            });
        });

        Assert.Equal(CircuitBreakerState.Open, breaker.State);

        // Wait for open duration to expire
        await Task.Delay(100);

        // State should now be HalfOpen
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);
    }

    [Fact]
    public async Task ExecuteAsync_HalfOpenSuccess_ClosesCircuit()
    {
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMilliseconds(50)
        };
        var breaker = new CircuitBreaker(options);

        // Trigger open state
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await breaker.ExecuteAsync<int>(async _ =>
            {
                await Task.Yield();
                throw new InvalidOperationException();
            });
        });

        // Wait for half-open
        await Task.Delay(100);
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);

        // Successful call should close
        var result = await breaker.ExecuteAsync(async _ =>
        {
            await Task.Yield();
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        Assert.Equal(0, breaker.FailureCount);
    }

    [Fact]
    public async Task ExecuteAsync_HalfOpenFailure_ReopensCircuit()
    {
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMilliseconds(50)
        };
        var breaker = new CircuitBreaker(options);

        // Trigger open state
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await breaker.ExecuteAsync<int>(async _ =>
            {
                await Task.Yield();
                throw new InvalidOperationException();
            });
        });

        // Wait for half-open
        await Task.Delay(100);
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);

        // Failure in half-open should reopen
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await breaker.ExecuteAsync<int>(async _ =>
            {
                await Task.Yield();
                throw new InvalidOperationException();
            });
        });

        Assert.Equal(CircuitBreakerState.Open, breaker.State);
    }

    [Fact]
    public void Reset_ResetsAllState()
    {
        var options = new CircuitBreakerOptions { FailureThreshold = 1 };
        var breaker = new CircuitBreaker(options);

        // Record a failure to open the circuit
        breaker.RecordFailure();
        Assert.Equal(CircuitBreakerState.Open, breaker.State);

        // Reset should close
        breaker.Reset();

        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        Assert.Equal(0, breaker.FailureCount);
    }

    [Fact]
    public async Task ExecuteAsync_FailureWindowExpires_ResetsCount()
    {
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 5,
            FailureWindow = TimeSpan.FromMilliseconds(50)
        };
        var breaker = new CircuitBreaker(options);

        // Record 3 failures
        for (var i = 0; i < 3; i++)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await breaker.ExecuteAsync<int>(async _ =>
                {
                    await Task.Yield();
                    throw new InvalidOperationException();
                });
            });
        }

        Assert.Equal(3, breaker.FailureCount);

        // Wait for failure window to expire
        await Task.Delay(100);

        // Next failure should start fresh
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await breaker.ExecuteAsync<int>(async _ =>
            {
                await Task.Yield();
                throw new InvalidOperationException();
            });
        });

        Assert.Equal(1, breaker.FailureCount);
        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
    }

    [Fact]
    public async Task ExecuteAsync_SuccessResetsFailureCount()
    {
        var options = new CircuitBreakerOptions { FailureThreshold = 5 };
        var breaker = new CircuitBreaker(options);

        // Record 3 failures
        for (var i = 0; i < 3; i++)
        {
            breaker.RecordFailure();
        }

        Assert.Equal(3, breaker.FailureCount);

        // Successful call should reset
        await breaker.ExecuteAsync(async _ =>
        {
            await Task.Yield();
            return 42;
        });

        Assert.Equal(0, breaker.FailureCount);
    }

    [Fact]
    public void AllowRequest_ReturnsCorrectly()
    {
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMinutes(5)
        };
        var breaker = new CircuitBreaker(options);

        Assert.True(breaker.AllowRequest());

        breaker.RecordFailure();
        Assert.Equal(CircuitBreakerState.Open, breaker.State);
        Assert.False(breaker.AllowRequest());
    }

    [Fact]
    public async Task ExecuteAsync_CancellationNotCountedAsFailure()
    {
        var breaker = new CircuitBreaker();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await breaker.ExecuteAsync(async ct =>
            {
                ct.ThrowIfCancellationRequested();
                await Task.Yield();
                return 42;
            }, cts.Token);
        });

        Assert.Equal(0, breaker.FailureCount);
        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
    }

    [Fact]
    public async Task ExecuteAsync_VoidOverload_Works()
    {
        var breaker = new CircuitBreaker();
        var executed = false;

        await breaker.ExecuteAsync(async _ =>
        {
            await Task.Yield();
            executed = true;
        });

        Assert.True(executed);
    }

    [Fact]
    public void RecordSuccess_InHalfOpen_ClosesCircuit()
    {
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMilliseconds(1)
        };
        var breaker = new CircuitBreaker(options);

        breaker.RecordFailure();
        Assert.Equal(CircuitBreakerState.Open, breaker.State);

        Thread.Sleep(50);

        // Access state to trigger transition to HalfOpen
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);

        breaker.RecordSuccess();
        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
    }

    [Fact]
    public void CircuitBreakerOpenException_Create_SetsTimeUntilReset()
    {
        var timeUntilReset = TimeSpan.FromSeconds(30);
        var ex = CircuitBreakerOpenException.Create(timeUntilReset);

        Assert.Equal(timeUntilReset, ex.TimeUntilReset);
        Assert.Contains("30", ex.Message);
    }

    [Fact]
    public void CircuitBreakerOpenException_ForServer_SetsHostAndPort()
    {
        var ex = CircuitBreakerOpenException.ForServer("localhost", 9000, TimeSpan.FromSeconds(15));

        Assert.Equal("localhost", ex.Host);
        Assert.Equal(9000, ex.Port);
        Assert.Equal(TimeSpan.FromSeconds(15), ex.TimeUntilReset);
        Assert.Contains("localhost:9000", ex.Message);
    }

    [Fact]
    public async Task State_IsThreadSafe()
    {
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 100,
            OpenDuration = TimeSpan.FromMilliseconds(10)
        };
        var breaker = new CircuitBreaker(options);

        var tasks = new List<Task>();

        // Multiple threads recording failures and successes
        for (var i = 0; i < 10; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (var j = 0; j < 50; j++)
                {
                    breaker.RecordFailure();
                    _ = breaker.State;
                    _ = breaker.FailureCount;
                }
            }));

            tasks.Add(Task.Run(() =>
            {
                for (var j = 0; j < 50; j++)
                {
                    breaker.RecordSuccess();
                    _ = breaker.State;
                    _ = breaker.AllowRequest();
                }
            }));
        }

        await Task.WhenAll(tasks);

        // Should complete without exceptions - state may vary based on timing
        var state = breaker.State;
        Assert.True(state is CircuitBreakerState.Closed or CircuitBreakerState.Open or CircuitBreakerState.HalfOpen);
    }

    [Fact]
    public async Task OnStateChanged_Event_RaisedOnOpen()
    {
        var options = new CircuitBreakerOptions { FailureThreshold = 2 };
        var breaker = new CircuitBreaker(options);
        var stateChanges = new List<CircuitBreakerStateChangedEventArgs>();

        breaker.OnStateChanged += (_, args) => stateChanges.Add(args);

        breaker.RecordFailure();
        breaker.RecordFailure(); // Should trigger Open

        // Wait for async event dispatch
        await Task.Delay(50);

        Assert.Single(stateChanges);
        Assert.Equal(CircuitBreakerState.Closed, stateChanges[0].OldState);
        Assert.Equal(CircuitBreakerState.Open, stateChanges[0].NewState);
    }

    [Fact]
    public async Task OnStateChanged_Event_RaisedOnHalfOpen()
    {
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMilliseconds(10)
        };
        var breaker = new CircuitBreaker(options);
        var stateChanges = new List<CircuitBreakerStateChangedEventArgs>();

        breaker.OnStateChanged += (_, args) => stateChanges.Add(args);

        breaker.RecordFailure(); // Triggers Open

        await Task.Delay(50); // Wait for HalfOpen transition to become available

        _ = breaker.State; // Triggers HalfOpen transition

        // Wait for async event dispatch
        await Task.Delay(50);

        Assert.Equal(2, stateChanges.Count);
        Assert.Equal(CircuitBreakerState.Open, stateChanges[1].OldState);
        Assert.Equal(CircuitBreakerState.HalfOpen, stateChanges[1].NewState);
    }

    [Fact]
    public async Task OnStateChanged_Event_RaisedOnReset()
    {
        var options = new CircuitBreakerOptions { FailureThreshold = 1 };
        var breaker = new CircuitBreaker(options);
        var stateChanges = new List<CircuitBreakerStateChangedEventArgs>();

        breaker.RecordFailure(); // Triggers Open

        breaker.OnStateChanged += (_, args) => stateChanges.Add(args);

        breaker.Reset();

        // Wait for async event dispatch
        await Task.Delay(50);

        Assert.Single(stateChanges);
        Assert.Equal(CircuitBreakerState.Open, stateChanges[0].OldState);
        Assert.Equal(CircuitBreakerState.Closed, stateChanges[0].NewState);
    }

    [Fact]
    public async Task OnStateChanged_Event_RaisedOnSuccessInHalfOpen()
    {
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMilliseconds(10)
        };
        var breaker = new CircuitBreaker(options);

        breaker.RecordFailure(); // Triggers Open

        await Task.Delay(50); // Wait for HalfOpen transition

        var stateChanges = new List<CircuitBreakerStateChangedEventArgs>();
        breaker.OnStateChanged += (_, args) => stateChanges.Add(args);

        // Trigger HalfOpen by accessing state, then success to close
        _ = breaker.State;
        breaker.RecordSuccess();

        // Wait for async event dispatch
        await Task.Delay(50);

        // Should have HalfOpen transition and then Closed transition
        Assert.True(stateChanges.Count >= 1);
        var closeEvent = stateChanges.FirstOrDefault(e => e.NewState == CircuitBreakerState.Closed);
        Assert.NotNull(closeEvent);
    }
}
