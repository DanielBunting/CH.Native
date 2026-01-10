namespace CH.Native.Protocol.Messages;

/// <summary>
/// Marker message indicating the end of the server response stream.
/// </summary>
public sealed class EndOfStreamMessage
{
    /// <summary>
    /// Singleton instance.
    /// </summary>
    public static EndOfStreamMessage Instance { get; } = new EndOfStreamMessage();

    private EndOfStreamMessage() { }
}
