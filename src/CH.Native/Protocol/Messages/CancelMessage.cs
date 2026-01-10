namespace CH.Native.Protocol.Messages;

/// <summary>
/// Cancel message sent to abort a running query on the server.
/// </summary>
public static class CancelMessage
{
    /// <summary>
    /// Writes the cancel message to the protocol writer.
    /// The cancel message consists only of the message type byte (0x03) with no payload.
    /// </summary>
    /// <param name="writer">The protocol writer to write to.</param>
    public static void Write(ref ProtocolWriter writer)
    {
        writer.WriteVarInt((ulong)ClientMessageType.Cancel);
    }
}
