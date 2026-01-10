namespace CH.Native.Resilience;

/// <summary>
/// Configuration options for connection resilience features.
/// </summary>
public sealed record ResilienceOptions
{
    /// <summary>
    /// Default resilience options with no retry or circuit breaker enabled.
    /// </summary>
    public static readonly ResilienceOptions Default = new();

    /// <summary>
    /// Gets the retry options, or null to disable retry.
    /// </summary>
    public RetryOptions? Retry { get; init; }

    /// <summary>
    /// Gets the circuit breaker options, or null to disable circuit breaker.
    /// </summary>
    public CircuitBreakerOptions? CircuitBreaker { get; init; }

    /// <summary>
    /// Gets the interval between health checks. Default is 10 seconds.
    /// </summary>
    public TimeSpan HealthCheckInterval { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Gets whether retry is enabled.
    /// </summary>
    public bool HasRetry => Retry != null;

    /// <summary>
    /// Gets whether circuit breaker is enabled.
    /// </summary>
    public bool HasCircuitBreaker => CircuitBreaker != null;

    /// <summary>
    /// Creates resilience options with retry enabled using default settings.
    /// </summary>
    public static ResilienceOptions WithRetryDefaults() => new()
    {
        Retry = RetryOptions.Default
    };

    /// <summary>
    /// Creates resilience options with circuit breaker enabled using default settings.
    /// </summary>
    public static ResilienceOptions WithCircuitBreakerDefaults() => new()
    {
        CircuitBreaker = CircuitBreakerOptions.Default
    };

    /// <summary>
    /// Creates resilience options with both retry and circuit breaker enabled using default settings.
    /// </summary>
    public static ResilienceOptions WithAllDefaults() => new()
    {
        Retry = RetryOptions.Default,
        CircuitBreaker = CircuitBreakerOptions.Default
    };
}
