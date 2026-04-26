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

    /// <summary>
    /// Non-allocating, non-throwing scan that returns true iff the bytes for a complete
    /// ExceptionMessage (and any nested exceptions) are present in the reader. Mirrors
    /// <see cref="Read"/> field-for-field — keep them in sync. The reader's position is
    /// unspecified on false; callers must rebuild a fresh reader from the original
    /// buffer position once more bytes arrive (see ProtocolReader's Try* contract).
    ///
    /// <para>Why we need a separate scan pass: <see cref="Read"/> throws on incomplete
    /// data, which used to be caught at the connection-pump catch-all and treated as
    /// "not enough bytes". That worked but conflated incomplete-data with any other
    /// <see cref="InvalidOperationException"/> the parser might throw. The scan pass
    /// makes the contract explicit: false means incomplete (caller pumps more bytes),
    /// throw means malformed (caller tears the connection down).</para>
    /// </summary>
    public static bool TryScan(ref ProtocolReader reader)
    {
        // Code = Int32 (4 bytes LE).
        if (!reader.TryReadInt32(out _))
            return false;

        // Name, message, stack trace — three VarInt-prefixed strings.
        if (!reader.TrySkipString())
            return false;
        if (!reader.TrySkipString())
            return false;
        if (!reader.TrySkipString())
            return false;

        // hasNested byte.
        if (!reader.TryReadByte(out var hasNested))
            return false;

        // Recurse into nested exception if present.
        if (hasNested != 0)
        {
            if (!TryScan(ref reader))
                return false;
        }

        return true;
    }
}
