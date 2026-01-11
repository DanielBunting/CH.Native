namespace CH.Native.Resilience;

/// <summary>
/// Represents the state of a circuit breaker.
/// </summary>
public enum CircuitBreakerState
{
    /// <summary>
    /// Circuit is closed - requests pass through normally.
    /// </summary>
    Closed,

    /// <summary>
    /// Circuit is open - requests are rejected immediately.
    /// </summary>
    Open,

    /// <summary>
    /// Circuit is half-open - testing if the service has recovered.
    /// </summary>
    HalfOpen
}
