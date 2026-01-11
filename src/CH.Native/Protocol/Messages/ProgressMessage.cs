using CH.Native.Data;

namespace CH.Native.Protocol.Messages;

/// <summary>
/// Server progress message containing query execution statistics.
/// </summary>
public readonly struct ProgressMessage
{
    /// <summary>
    /// Gets the number of rows processed.
    /// </summary>
    public ulong Rows { get; init; }

    /// <summary>
    /// Gets the number of bytes processed.
    /// </summary>
    public ulong Bytes { get; init; }

    /// <summary>
    /// Gets the total number of rows to process (estimate).
    /// </summary>
    public ulong TotalRows { get; init; }

    /// <summary>
    /// Gets the number of rows written (for INSERT operations).
    /// </summary>
    public ulong WrittenRows { get; init; }

    /// <summary>
    /// Gets the number of bytes written (for INSERT operations).
    /// </summary>
    public ulong WrittenBytes { get; init; }

    /// <summary>
    /// Reads a ProgressMessage from the protocol reader.
    /// The message type should already have been read.
    /// </summary>
    /// <param name="reader">The protocol reader.</param>
    /// <param name="protocolRevision">The negotiated protocol revision.</param>
    /// <returns>The parsed ProgressMessage.</returns>
    public static ProgressMessage Read(ref ProtocolReader reader, int protocolRevision)
    {
        var rows = reader.ReadVarInt();
        var bytes = reader.ReadVarInt();
        var totalRows = reader.ReadVarInt();

        // Total bytes added in protocol version 54463
        if (protocolRevision >= ProtocolVersion.WithTotalBytesInProgress)
        {
            _ = reader.ReadVarInt(); // total_bytes_to_read - we don't use it currently
        }

        ulong writtenRows = 0;
        ulong writtenBytes = 0;

        // Written rows/bytes added in later protocol versions
        if (protocolRevision >= ProtocolVersion.WithClientWriteInfo)
        {
            writtenRows = reader.ReadVarInt();
            writtenBytes = reader.ReadVarInt();
        }

        // Elapsed time added in even later protocol versions
        if (protocolRevision >= ProtocolVersion.WithServerQueryTimeInProgress)
        {
            _ = reader.ReadVarInt(); // elapsed_ns - we don't use it currently
        }

        return new ProgressMessage
        {
            Rows = rows,
            Bytes = bytes,
            TotalRows = totalRows,
            WrittenRows = writtenRows,
            WrittenBytes = writtenBytes
        };
    }

    /// <summary>
    /// Converts this progress message to a QueryProgress for public API.
    /// </summary>
    /// <returns>A QueryProgress instance.</returns>
    public QueryProgress ToQueryProgress()
    {
        return new QueryProgress(
            RowsRead: Rows,
            BytesRead: Bytes,
            TotalRowsToRead: TotalRows,
            RowsWritten: WrittenRows,
            BytesWritten: WrittenBytes);
    }
}
