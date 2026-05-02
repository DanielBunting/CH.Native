using CH.Native.Protocol.Messages;

namespace CH.Native.Exceptions;

/// <summary>
/// Exception thrown when the ClickHouse server returns an error.
/// </summary>
public class ClickHouseServerException : ClickHouseException
{
    /// <summary>
    /// Gets the ClickHouse error code.
    /// </summary>
    public int ErrorCode { get; }

    /// <summary>
    /// Gets the ClickHouse exception class name.
    /// </summary>
    public string ServerExceptionName { get; }

    /// <summary>
    /// Gets the server-side stack trace.
    /// </summary>
    public string ServerStackTrace { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseServerException"/> class.
    /// </summary>
    /// <param name="errorCode">The ClickHouse error code.</param>
    /// <param name="serverExceptionName">The server exception class name.</param>
    /// <param name="message">The error message.</param>
    /// <param name="serverStackTrace">The server-side stack trace.</param>
    public ClickHouseServerException(int errorCode, string serverExceptionName, string message, string serverStackTrace)
        : base(FormatMessage(errorCode, serverExceptionName, message))
    {
        ErrorCode = errorCode;
        ServerExceptionName = serverExceptionName;
        ServerStackTrace = serverStackTrace;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseServerException"/> class.
    /// </summary>
    /// <param name="errorCode">The ClickHouse error code.</param>
    /// <param name="serverExceptionName">The server exception class name.</param>
    /// <param name="message">The error message.</param>
    /// <param name="serverStackTrace">The server-side stack trace.</param>
    /// <param name="innerException">The inner exception.</param>
    public ClickHouseServerException(int errorCode, string serverExceptionName, string message, string serverStackTrace, Exception innerException)
        : base(FormatMessage(errorCode, serverExceptionName, message), innerException)
    {
        ErrorCode = errorCode;
        ServerExceptionName = serverExceptionName;
        ServerStackTrace = serverStackTrace;
    }

    /// <summary>
    /// Creates a typed ClickHouse exception from an <see cref="ExceptionMessage"/>.
    /// Authentication / authorization error codes (192, 193, 194, 195, 196, 516)
    /// surface as <see cref="ClickHouseAuthenticationException"/> so the
    /// resilience layer's auth-non-transient classifier short-circuits
    /// retries — pre-fix mid-session auth failures were typed as plain
    /// <see cref="ClickHouseServerException"/> and routed through the
    /// transient retry path, defeating the Round-1 handshake fix.
    /// </summary>
    /// <param name="exceptionMessage">The exception message from the server.</param>
    /// <returns>A typed exception — <see cref="ClickHouseAuthenticationException"/>
    /// for auth codes, <see cref="ClickHouseServerException"/> otherwise.</returns>
    public static ClickHouseException FromExceptionMessage(ExceptionMessage exceptionMessage)
    {
        var inner = exceptionMessage.Nested != null
            ? FromExceptionMessage(exceptionMessage.Nested)
            : null;

        if (IsAuthenticationErrorCode(exceptionMessage.Code))
        {
            var authMsg = $"Server reported auth failure mid-session: " +
                $"[{exceptionMessage.Code}] {exceptionMessage.Name}: {exceptionMessage.Message}";
            return inner is null
                ? new ClickHouseAuthenticationException(authMsg)
                {
                    ErrorCode = exceptionMessage.Code,
                    ServerExceptionName = exceptionMessage.Name,
                }
                : new ClickHouseAuthenticationException(authMsg, inner)
                {
                    ErrorCode = exceptionMessage.Code,
                    ServerExceptionName = exceptionMessage.Name,
                };
        }

        return inner is null
            ? new ClickHouseServerException(
                exceptionMessage.Code,
                exceptionMessage.Name,
                exceptionMessage.Message,
                exceptionMessage.StackTrace)
            : new ClickHouseServerException(
                exceptionMessage.Code,
                exceptionMessage.Name,
                exceptionMessage.Message,
                exceptionMessage.StackTrace,
                inner);
    }

    // Mirrors ServerHello.IsAuthenticationErrorCode — kept in sync with the
    // ClickHouse src/Common/ErrorCodes.cpp permanent-auth-failure set.
    private static bool IsAuthenticationErrorCode(int code) => code switch
    {
        192 => true, // UNKNOWN_USER
        193 => true, // WRONG_PASSWORD
        194 => true, // REQUIRED_PASSWORD
        195 => true, // IP_ADDRESS_NOT_ALLOWED
        196 => true, // UNKNOWN_ADDRESS_PATTERN_TYPE
        516 => true, // AUTHENTICATION_FAILED
        _ => false,
    };

    private static string FormatMessage(int errorCode, string serverExceptionName, string message)
    {
        return $"ClickHouse error {errorCode} ({serverExceptionName}): {message}";
    }
}
