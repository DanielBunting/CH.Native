namespace CH.Native.Resilience;

/// <summary>
/// Event arguments for retry attempts.
/// </summary>
public sealed class RetryEventArgs : EventArgs
{
    /// <summary>
    /// Gets the current retry attempt number (1-based).
    /// </summary>
    public int AttemptNumber { get; }

    /// <summary>
    /// Gets the maximum number of retries configured.
    /// </summary>
    public int MaxRetries { get; }

    /// <summary>
    /// Gets the exception that caused the retry.
    /// </summary>
    public Exception Exception { get; }

    /// <summary>
    /// Gets the delay before the next retry attempt.
    /// </summary>
    public TimeSpan Delay { get; }

    /// <summary>
    /// Creates a new retry event args instance.
    /// </summary>
    public RetryEventArgs(int attemptNumber, int maxRetries, Exception exception, TimeSpan delay)
    {
        AttemptNumber = attemptNumber;
        MaxRetries = maxRetries;
        Exception = exception;
        Delay = delay;
    }
}

/// <summary>
/// Event arguments for circuit breaker state changes.
/// </summary>
public sealed class CircuitBreakerStateChangedEventArgs : EventArgs
{
    /// <summary>
    /// Gets the previous state of the circuit breaker.
    /// </summary>
    public CircuitBreakerState OldState { get; }

    /// <summary>
    /// Gets the new state of the circuit breaker.
    /// </summary>
    public CircuitBreakerState NewState { get; }

    /// <summary>
    /// Gets the failure count at the time of the state change.
    /// </summary>
    public int FailureCount { get; }

    /// <summary>
    /// Gets the timestamp of the state change.
    /// </summary>
    public DateTime Timestamp { get; }

    /// <summary>
    /// Creates a new circuit breaker state changed event args instance.
    /// </summary>
    public CircuitBreakerStateChangedEventArgs(
        CircuitBreakerState oldState,
        CircuitBreakerState newState,
        int failureCount)
    {
        OldState = oldState;
        NewState = newState;
        FailureCount = failureCount;
        Timestamp = DateTime.UtcNow;
    }
}

/// <summary>
/// Event arguments for health check completions.
/// </summary>
public sealed class HealthCheckCompletedEventArgs : EventArgs
{
    /// <summary>
    /// Gets the server address that was checked.
    /// </summary>
    public ServerAddress Server { get; }

    /// <summary>
    /// Gets whether the server is healthy.
    /// </summary>
    public bool IsHealthy { get; }

    /// <summary>
    /// Gets the previous health status before this check.
    /// </summary>
    public bool WasHealthy { get; }

    /// <summary>
    /// Gets the duration of the health check.
    /// </summary>
    public TimeSpan Duration { get; }

    /// <summary>
    /// Gets the exception that occurred during the health check, if any.
    /// </summary>
    public Exception? Exception { get; }

    /// <summary>
    /// Gets the timestamp of the health check.
    /// </summary>
    public DateTime Timestamp { get; }

    /// <summary>
    /// Creates a new health check completed event args instance.
    /// </summary>
    public HealthCheckCompletedEventArgs(
        ServerAddress server,
        bool isHealthy,
        bool wasHealthy,
        TimeSpan duration,
        Exception? exception = null)
    {
        Server = server;
        IsHealthy = isHealthy;
        WasHealthy = wasHealthy;
        Duration = duration;
        Exception = exception;
        Timestamp = DateTime.UtcNow;
    }
}
