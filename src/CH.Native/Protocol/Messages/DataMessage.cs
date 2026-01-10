using CH.Native.Data;

namespace CH.Native.Protocol.Messages;

/// <summary>
/// Server data message containing a block of query results.
/// </summary>
public sealed class DataMessage
{
    /// <summary>
    /// Gets the typed data block.
    /// </summary>
    public required TypedBlock Block { get; init; }

    /// <summary>
    /// Reads a DataMessage from the protocol reader.
    /// The message type should already have been read.
    /// </summary>
    /// <param name="reader">The protocol reader.</param>
    /// <param name="registry">The column reader registry.</param>
    /// <param name="protocolVersion">The negotiated protocol version.</param>
    /// <returns>The parsed DataMessage.</returns>
    public static DataMessage Read(ref ProtocolReader reader, ColumnReaderRegistry registry, int protocolVersion)
    {
        var tableName = reader.ReadString();
        var block = Data.Block.ReadTypedBlockWithTableName(ref reader, registry, tableName, protocolVersion);
        return new DataMessage { Block = block };
    }
}
