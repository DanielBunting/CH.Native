namespace CH.Native.Exceptions;

/// <summary>
/// Thrown when ClickHouse rejects a connection for authentication or authorization
/// reasons (wrong password, unknown user, IP not allowed, TLS authentication failure).
/// Authentication failures are <em>permanent</em> — retrying them with the same
/// credentials cannot recover the connection — so the resilience layer
/// short-circuits any retry policy when this exception is observed.
/// </summary>
/// <remarks>
/// Inherits from <see cref="ClickHouseConnectionException"/> so existing
/// <c>catch (ClickHouseConnectionException)</c> sites continue to handle it; the
/// distinct type only adds a way to opt out of retry classification.
/// </remarks>
public class ClickHouseAuthenticationException : ClickHouseConnectionException
{
    /// <summary>
    /// Server-side error code, when the failure originated from a ClickHouse
    /// exception message during handshake (e.g. 192 / 516 — AUTHENTICATION_FAILED,
    /// 193 — WRONG_PASSWORD). <see langword="null"/> when the failure is
    /// client-side (e.g. TLS handshake / certificate validation).
    /// </summary>
    public int? ErrorCode { get; init; }

    /// <summary>
    /// Server-side exception class name, when available.
    /// </summary>
    public string? ServerExceptionName { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseAuthenticationException"/> class.
    /// </summary>
    public ClickHouseAuthenticationException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseAuthenticationException"/> class with a message.
    /// </summary>
    /// <param name="message">The error message.</param>
    public ClickHouseAuthenticationException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseAuthenticationException"/> class with a message and inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public ClickHouseAuthenticationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
