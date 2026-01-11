namespace CH.Native.Resilience;

/// <summary>
/// Configuration options for circuit breaker behavior.
/// </summary>
public sealed record CircuitBreakerOptions
{
    /// <summary>
    /// Default circuit breaker options with 5 failure threshold, 30s open duration, and 1 minute failure window.
    /// </summary>
    public static readonly CircuitBreakerOptions Default = new();

    /// <summary>
    /// Gets the number of failures required to open the circuit. Default is 5.
    /// </summary>
    public int FailureThreshold { get; init; } = 5;

    /// <summary>
    /// Gets the duration the circuit stays open before transitioning to half-open. Default is 30 seconds.
    /// </summary>
    public TimeSpan OpenDuration { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets the time window for counting failures. Failures outside this window are not counted. Default is 1 minute.
    /// </summary>
    public TimeSpan FailureWindow { get; init; } = TimeSpan.FromMinutes(1);
}
