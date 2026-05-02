namespace CH.Native.Protocol.Messages;

/// <summary>
/// Sentinel returned by <c>TryReadMessage</c> for server-pushed messages that
/// must be drained from the wire but not surfaced to the caller (e.g.
/// <c>ReadTaskRequest</c>, <c>PartUUIDs</c>). The outer read loop recognises
/// the sentinel via reference equality and continues iterating instead of
/// yielding it.
/// </summary>
internal sealed class SkipMessage
{
    public static readonly SkipMessage Instance = new();
    private SkipMessage() { }
}
