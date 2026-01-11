namespace CH.Native.Exceptions;

/// <summary>
/// Exception thrown when a circuit breaker is open and rejecting requests.
/// </summary>
public class CircuitBreakerOpenException : ClickHouseConnectionException
{
    /// <summary>
    /// Gets the time remaining until the circuit breaker will transition to half-open.
    /// </summary>
    public TimeSpan TimeUntilReset { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="CircuitBreakerOpenException"/> class.
    /// </summary>
    public CircuitBreakerOpenException()
        : base("Circuit breaker is open - requests not allowed")
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CircuitBreakerOpenException"/> class with a message.
    /// </summary>
    /// <param name="message">The error message.</param>
    public CircuitBreakerOpenException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Creates a circuit breaker open exception with time until reset information.
    /// </summary>
    /// <param name="timeUntilReset">The time remaining until the circuit will try to reset.</param>
    /// <returns>A new circuit breaker open exception.</returns>
    public static CircuitBreakerOpenException Create(TimeSpan timeUntilReset)
    {
        return new CircuitBreakerOpenException(
            $"Circuit breaker is open. Will attempt reset in {timeUntilReset.TotalSeconds:F0} seconds.")
        {
            TimeUntilReset = timeUntilReset
        };
    }

    /// <summary>
    /// Creates a circuit breaker open exception for a specific server.
    /// </summary>
    /// <param name="host">The server host.</param>
    /// <param name="port">The server port.</param>
    /// <param name="timeUntilReset">The time remaining until the circuit will try to reset.</param>
    /// <returns>A new circuit breaker open exception.</returns>
    public static CircuitBreakerOpenException ForServer(string host, int port, TimeSpan timeUntilReset)
    {
        return new CircuitBreakerOpenException(
            $"Circuit breaker is open for {host}:{port}. Will attempt reset in {timeUntilReset.TotalSeconds:F0} seconds.")
        {
            Host = host,
            Port = port,
            TimeUntilReset = timeUntilReset
        };
    }
}
