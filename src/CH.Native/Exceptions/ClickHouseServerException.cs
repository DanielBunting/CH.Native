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
    /// Creates a ClickHouseServerException from an ExceptionMessage.
    /// </summary>
    /// <param name="exceptionMessage">The exception message from the server.</param>
    /// <returns>A new ClickHouseServerException.</returns>
    public static ClickHouseServerException FromExceptionMessage(ExceptionMessage exceptionMessage)
    {
        if (exceptionMessage.Nested != null)
        {
            var innerException = FromExceptionMessage(exceptionMessage.Nested);
            return new ClickHouseServerException(
                exceptionMessage.Code,
                exceptionMessage.Name,
                exceptionMessage.Message,
                exceptionMessage.StackTrace,
                innerException);
        }

        return new ClickHouseServerException(
            exceptionMessage.Code,
            exceptionMessage.Name,
            exceptionMessage.Message,
            exceptionMessage.StackTrace);
    }

    private static string FormatMessage(int errorCode, string serverExceptionName, string message)
    {
        return $"ClickHouse error {errorCode} ({serverExceptionName}): {message}";
    }
}
