using CH.Native.Exceptions;
using CH.Native.Telemetry;

namespace CH.Native.Resilience;

/// <summary>
/// Implements the circuit breaker pattern for fault tolerance.
/// </summary>
/// <remarks>
/// <para>
/// The circuit breaker has three states:
/// </para>
/// <list type="bullet">
/// <item><description>Closed: Normal operation, requests pass through</description></item>
/// <item><description>Open: Failures exceeded threshold, requests are rejected immediately</description></item>
/// <item><description>HalfOpen: Testing if the service has recovered</description></item>
/// </list>
/// </remarks>
public sealed class CircuitBreaker
{
    private readonly CircuitBreakerOptions _options;
    private readonly object _lock = new();

    private CircuitBreakerState _state = CircuitBreakerState.Closed;
    private DateTime _lastStateChange = DateTime.UtcNow;
    private int _failureCount;
    private DateTime _windowStart = DateTime.UtcNow;

    /// <summary>
    /// Gets or sets the server address for telemetry labeling.
    /// </summary>
    public string? ServerAddress { get; set; }

    /// <summary>
    /// Occurs when the circuit breaker state changes.
    /// </summary>
    public event EventHandler<CircuitBreakerStateChangedEventArgs>? OnStateChanged;

    /// <summary>
    /// Gets the current state of the circuit breaker.
    /// </summary>
    /// <remarks>
    /// Reading this property may cause a state transition from Open to HalfOpen
    /// if the open duration has expired.
    /// </remarks>
    public CircuitBreakerState State
    {
        get
        {
            lock (_lock)
            {
                CheckForStateTransition();
                return _state;
            }
        }
    }

    /// <summary>
    /// Gets the number of consecutive failures in the current window.
    /// </summary>
    public int FailureCount
    {
        get
        {
            lock (_lock)
            {
                return _failureCount;
            }
        }
    }

    /// <summary>
    /// Creates a new circuit breaker with the specified options.
    /// </summary>
    /// <param name="options">The circuit breaker options, or null to use defaults.</param>
    public CircuitBreaker(CircuitBreakerOptions? options = null)
    {
        _options = options ?? CircuitBreakerOptions.Default;
    }

    /// <summary>
    /// Executes an action through the circuit breaker.
    /// </summary>
    /// <typeparam name="T">The return type of the action.</typeparam>
    /// <param name="action">The action to execute.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result of the action.</returns>
    /// <exception cref="CircuitBreakerOpenException">Thrown when the circuit is open.</exception>
    public async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> action,
        CancellationToken cancellationToken = default)
    {
        EnsureNotOpen();

        try
        {
            var result = await action(cancellationToken).ConfigureAwait(false);
            OnSuccess();
            return result;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Don't count cancellation as a failure
            throw;
        }
        catch (Exception)
        {
            OnFailure();
            throw;
        }
    }

    /// <summary>
    /// Executes an action through the circuit breaker (void return).
    /// </summary>
    /// <param name="action">The action to execute.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="CircuitBreakerOpenException">Thrown when the circuit is open.</exception>
    public async Task ExecuteAsync(
        Func<CancellationToken, Task> action,
        CancellationToken cancellationToken = default)
    {
        await ExecuteAsync(async ct =>
        {
            await action(ct).ConfigureAwait(false);
            return true;
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Manually resets the circuit breaker to the closed state.
    /// </summary>
    public void Reset()
    {
        CircuitBreakerState? oldState = null;

        lock (_lock)
        {
            if (_state != CircuitBreakerState.Closed)
            {
                oldState = _state;
            }
            _state = CircuitBreakerState.Closed;
            _failureCount = 0;
            _windowStart = DateTime.UtcNow;
            _lastStateChange = DateTime.UtcNow;
        }

        if (oldState.HasValue)
        {
            RaiseStateChanged(oldState.Value, CircuitBreakerState.Closed);
        }
    }

    /// <summary>
    /// Records a successful operation. Call this when an operation succeeds outside of ExecuteAsync.
    /// </summary>
    public void RecordSuccess()
    {
        OnSuccess();
    }

    /// <summary>
    /// Records a failed operation. Call this when an operation fails outside of ExecuteAsync.
    /// </summary>
    public void RecordFailure()
    {
        OnFailure();
    }

    /// <summary>
    /// Checks if a request is currently allowed by the circuit breaker.
    /// </summary>
    /// <returns>True if requests are allowed, false if the circuit is open.</returns>
    public bool AllowRequest()
    {
        lock (_lock)
        {
            CheckForStateTransition();
            return _state != CircuitBreakerState.Open;
        }
    }

    private void EnsureNotOpen()
    {
        lock (_lock)
        {
            CheckForStateTransition();

            if (_state == CircuitBreakerState.Open)
            {
                var elapsed = DateTime.UtcNow - _lastStateChange;
                var remaining = _options.OpenDuration - elapsed;
                throw CircuitBreakerOpenException.Create(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
            }
        }
    }

    private void CheckForStateTransition()
    {
        // Must be called under lock
        if (_state == CircuitBreakerState.Open)
        {
            var elapsed = DateTime.UtcNow - _lastStateChange;
            if (elapsed >= _options.OpenDuration)
            {
                var oldState = _state;
                _state = CircuitBreakerState.HalfOpen;
                _lastStateChange = DateTime.UtcNow;
                RaiseStateChanged(oldState, _state);
            }
        }
    }

    private void RaiseStateChanged(CircuitBreakerState oldState, CircuitBreakerState newState)
    {
        // Record telemetry for state transition
        var serverAddr = ServerAddress ?? "default";
        ClickHouseMeter.RecordCircuitBreakerTransition(serverAddr, oldState, newState);

        // Invoke outside the lock to prevent potential deadlocks
        var handler = OnStateChanged;
        if (handler != null)
        {
            Task.Run(() => handler.Invoke(this, new CircuitBreakerStateChangedEventArgs(oldState, newState, _failureCount)));
        }
    }

    private void OnSuccess()
    {
        CircuitBreakerState? oldState = null;
        CircuitBreakerState? newState = null;

        lock (_lock)
        {
            if (_state == CircuitBreakerState.HalfOpen)
            {
                // Recovery confirmed, close the circuit
                oldState = _state;
                _state = CircuitBreakerState.Closed;
                newState = _state;
                _failureCount = 0;
                _windowStart = DateTime.UtcNow;
                _lastStateChange = DateTime.UtcNow;
            }
            else if (_state == CircuitBreakerState.Closed)
            {
                // Reset failure count on success while closed
                _failureCount = 0;
                _windowStart = DateTime.UtcNow;
            }
        }

        if (oldState.HasValue && newState.HasValue)
        {
            RaiseStateChanged(oldState.Value, newState.Value);
        }
    }

    private void OnFailure()
    {
        CircuitBreakerState? oldState = null;
        CircuitBreakerState? newState = null;

        lock (_lock)
        {
            var now = DateTime.UtcNow;

            if (_state == CircuitBreakerState.HalfOpen)
            {
                // Still failing, reopen the circuit
                oldState = _state;
                _state = CircuitBreakerState.Open;
                newState = _state;
                _lastStateChange = now;
            }
            else
            {
                // Check if we need to reset the window
                if (now - _windowStart > _options.FailureWindow)
                {
                    _failureCount = 0;
                    _windowStart = now;
                }

                _failureCount++;

                if (_failureCount >= _options.FailureThreshold)
                {
                    oldState = _state;
                    _state = CircuitBreakerState.Open;
                    newState = _state;
                    _lastStateChange = now;
                }
            }
        }

        if (oldState.HasValue && newState.HasValue)
        {
            RaiseStateChanged(oldState.Value, newState.Value);
        }
    }
}
