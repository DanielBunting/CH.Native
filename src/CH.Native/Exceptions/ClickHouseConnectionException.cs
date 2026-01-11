namespace CH.Native.Exceptions;

/// <summary>
/// Exception thrown when a connection to ClickHouse fails.
/// </summary>
public class ClickHouseConnectionException : ClickHouseException
{
    /// <summary>
    /// Gets the host that the connection was attempting to reach.
    /// </summary>
    public string? Host { get; init; }

    /// <summary>
    /// Gets the port that the connection was attempting to reach.
    /// </summary>
    public int? Port { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseConnectionException"/> class.
    /// </summary>
    public ClickHouseConnectionException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseConnectionException"/> class with a message.
    /// </summary>
    /// <param name="message">The error message.</param>
    public ClickHouseConnectionException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseConnectionException"/> class with a message and inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public ClickHouseConnectionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// Creates an exception for a connection timeout.
    /// </summary>
    /// <param name="host">The host that timed out.</param>
    /// <param name="port">The port that was attempted.</param>
    /// <param name="timeout">The timeout duration.</param>
    /// <returns>A new connection exception.</returns>
    public static ClickHouseConnectionException Timeout(string host, int port, TimeSpan timeout)
    {
        return new ClickHouseConnectionException(
            $"Connection to {host}:{port} timed out after {timeout.TotalSeconds:F1}s")
        {
            Host = host,
            Port = port
        };
    }

    /// <summary>
    /// Creates an exception for a refused connection.
    /// </summary>
    /// <param name="host">The host that refused the connection.</param>
    /// <param name="port">The port that was attempted.</param>
    /// <param name="innerException">The underlying socket exception.</param>
    /// <returns>A new connection exception.</returns>
    public static ClickHouseConnectionException Refused(string host, int port, Exception innerException)
    {
        return new ClickHouseConnectionException(
            $"Connection to {host}:{port} was refused", innerException)
        {
            Host = host,
            Port = port
        };
    }

    /// <summary>
    /// Creates an exception for a failed connection attempt.
    /// </summary>
    /// <param name="host">The host that failed.</param>
    /// <param name="port">The port that was attempted.</param>
    /// <param name="innerException">The underlying exception.</param>
    /// <returns>A new connection exception.</returns>
    public static ClickHouseConnectionException Failed(string host, int port, Exception innerException)
    {
        return new ClickHouseConnectionException(
            $"Failed to connect to {host}:{port}: {innerException.Message}", innerException)
        {
            Host = host,
            Port = port
        };
    }
}
