namespace CH.Native.Exceptions;

/// <summary>
/// Base exception for all ClickHouse-related errors.
/// </summary>
public class ClickHouseException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseException"/> class.
    /// </summary>
    public ClickHouseException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseException"/> class with a message.
    /// </summary>
    /// <param name="message">The error message.</param>
    public ClickHouseException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseException"/> class with a message and inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public ClickHouseException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
