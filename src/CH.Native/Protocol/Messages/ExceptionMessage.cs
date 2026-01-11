namespace CH.Native.Protocol.Messages;

/// <summary>
/// Server exception message containing error details.
/// </summary>
public sealed record ExceptionMessage
{
    /// <summary>
    /// Gets the error code.
    /// </summary>
    public required int Code { get; init; }

    /// <summary>
    /// Gets the exception class name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Gets the error message.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// Gets the server-side stack trace.
    /// </summary>
    public required string StackTrace { get; init; }

    /// <summary>
    /// Gets the nested exception, if any.
    /// </summary>
    public ExceptionMessage? Nested { get; init; }

    /// <summary>
    /// Reads an ExceptionMessage from the protocol reader.
    /// The message type should already have been read.
    /// </summary>
    /// <param name="reader">The protocol reader.</param>
    /// <returns>The parsed ExceptionMessage.</returns>
    public static ExceptionMessage Read(ref ProtocolReader reader)
    {
        var code = reader.ReadInt32();
        var name = reader.ReadString();
        var message = reader.ReadString();
        var stackTrace = reader.ReadString();
        var hasNested = reader.ReadByte() != 0;

        ExceptionMessage? nested = null;
        if (hasNested)
        {
            nested = Read(ref reader);
        }

        return new ExceptionMessage
        {
            Code = code,
            Name = name,
            Message = message,
            StackTrace = stackTrace,
            Nested = nested
        };
    }
}
