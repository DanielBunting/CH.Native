using System.Buffers;
using CH.Native.Compression;
using CH.Native.Protocol;

namespace CH.Native.Data;

/// <summary>
/// Represents a data block containing column data from ClickHouse.
/// </summary>
public sealed class Block
{
    /// <summary>
    /// Gets the table name (empty for result blocks).
    /// </summary>
    public string TableName { get; init; } = string.Empty;

    /// <summary>
    /// Gets the block metadata.
    /// </summary>
    public BlockInfo Info { get; init; }

    /// <summary>
    /// Gets the number of columns in this block.
    /// </summary>
    public int ColumnCount { get; init; }

    /// <summary>
    /// Gets the number of rows in this block.
    /// </summary>
    public int RowCount { get; init; }

    /// <summary>
    /// Gets the column names.
    /// </summary>
    public string[] ColumnNames { get; init; } = Array.Empty<string>();

    /// <summary>
    /// Gets the column type names as strings.
    /// </summary>
    public string[] ColumnTypes { get; init; } = Array.Empty<string>();

    /// <summary>
    /// Gets the column data (one array per column).
    /// </summary>
    public object?[][] Columns { get; init; } = Array.Empty<object?[]>();

    /// <summary>
    /// Gets whether this is an empty block (no rows).
    /// </summary>
    public bool IsEmpty => RowCount == 0;

    /// <summary>
    /// Creates an empty block for sending "no data" to the server.
    /// </summary>
    public static Block Empty { get; } = new Block
    {
        TableName = string.Empty,
        Info = BlockInfo.Default,
        ColumnCount = 0,
        RowCount = 0,
        ColumnNames = Array.Empty<string>(),
        ColumnTypes = Array.Empty<string>(),
        Columns = Array.Empty<object?[]>()
    };

    /// <summary>
    /// Gets a value from the block at the specified row and column.
    /// </summary>
    /// <param name="row">The row index.</param>
    /// <param name="column">The column index.</param>
    /// <returns>The value at the specified position.</returns>
    public object? GetValue(int row, int column)
    {
        if (row < 0 || row >= RowCount)
            throw new ArgumentOutOfRangeException(nameof(row));
        if (column < 0 || column >= ColumnCount)
            throw new ArgumentOutOfRangeException(nameof(column));

        return Columns[column][row];
    }

    /// <summary>
    /// Gets a typed value from the block at the specified row and column.
    /// </summary>
    /// <typeparam name="T">The expected type.</typeparam>
    /// <param name="row">The row index.</param>
    /// <param name="column">The column index.</param>
    /// <returns>The typed value at the specified position.</returns>
    public T? GetValue<T>(int row, int column)
    {
        var value = GetValue(row, column);
        if (value is null)
            return default;

        return (T)Convert.ChangeType(value, typeof(T));
    }

    /// <summary>
    /// Block header information (counts only, without column names/types).
    /// Used for early validation before committing to allocations.
    /// </summary>
    public readonly struct BlockHeader
    {
        /// <summary>The number of columns.</summary>
        public int ColumnCount { get; init; }

        /// <summary>The number of rows.</summary>
        public int RowCount { get; init; }
    }

    /// <summary>
    /// Reads only the block header (column count and row count) without parsing column data.
    /// This allows early validation of buffer size before committing to allocations.
    /// </summary>
    /// <param name="reader">The protocol reader positioned at the start of block info.</param>
    /// <returns>The block header, or null if insufficient data is available.</returns>
    public static BlockHeader? TryReadBlockHeader(ref ProtocolReader reader)
    {
        // Use non-throwing Try* methods to avoid exception overhead for incomplete data
        if (!BlockInfo.TryRead(ref reader, out _))
            return null;

        if (!reader.TryReadVarInt(out var columnCount))
            return null;

        if (!reader.TryReadVarInt(out var rowCount))
            return null;

        return new BlockHeader
        {
            ColumnCount = (int)columnCount,
            RowCount = (int)rowCount
        };
    }

    /// <summary>
    /// Estimates the minimum buffer size needed for a block based on row count.
    /// This is a conservative estimate used for early validation.
    /// </summary>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="columnCount">The number of columns.</param>
    /// <returns>The estimated minimum bytes needed.</returns>
    public static long EstimateMinBlockSize(int rowCount, int columnCount)
    {
        if (rowCount == 0 || columnCount == 0)
            return 0;

        // Conservative estimate: assume each column has at least 1 byte per row
        // This catches the obvious case of having way too little data
        return (long)rowCount * columnCount;
    }

    /// <summary>
    /// Tries to skip block column data without allocating arrays.
    /// This is the scan pass for two-pass parsing of uncompressed data.
    /// </summary>
    /// <param name="reader">The protocol reader positioned after BlockInfo/columnCount/rowCount.</param>
    /// <param name="skipperRegistry">The column skipper registry.</param>
    /// <param name="columnCount">The pre-read column count.</param>
    /// <param name="rowCount">The pre-read row count.</param>
    /// <param name="protocolVersion">The negotiated protocol version.</param>
    /// <returns>True if all column data was successfully skipped; false if not enough data available.</returns>
    public static bool TrySkipBlockColumns(
        ref ProtocolReader reader,
        ColumnSkipperRegistry skipperRegistry,
        int columnCount,
        int rowCount,
        int protocolVersion = 0)
    {
        if (columnCount == 0)
            return true;

        for (int i = 0; i < columnCount; i++)
        {
            // Skip column name (string)
            if (!reader.TrySkipString())
                return false;

            // Read column type (we need to know the type to skip the data correctly)
            // This allocates a small string but avoids the much larger data allocations
            if (!reader.TryReadVarInt(out var typeLength))
                return false;

            if (reader.Remaining < (long)typeLength)
                return false;

            // Read type bytes - try byte-based lookup first to avoid string allocation
            var typeBytes = reader.ReadBytes((int)typeLength);

            // Skip custom serialization metadata for protocol >= WithCustomSerialization
            if (protocolVersion >= ProtocolVersion.WithCustomSerialization)
            {
                if (!reader.TryReadByte(out var hasCustom))
                    return false;

                if (hasCustom != 0)
                {
                    // Custom serialization kind - skip it
                    if (!reader.TryReadByte(out _))
                        return false;
                }
            }

            // Skip column data
            if (rowCount > 0)
            {
                // Try byte-based lookup first (avoids string allocation for common types)
                var skipper = skipperRegistry.TryGetSkipperByBytes(typeBytes.Span);
                if (skipper == null)
                {
                    // Fallback to string-based lookup for complex/parameterized types
                    var columnType = System.Text.Encoding.UTF8.GetString(typeBytes.Span);
                    skipper = skipperRegistry.GetSkipper(columnType);
                }
                if (!skipper.TrySkipColumn(ref reader, rowCount))
                    return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Reads column headers and data using pre-read header info.
    /// Use this after TryReadBlockHeader to continue from where it left off.
    /// </summary>
    /// <param name="reader">The protocol reader positioned after BlockInfo/columnCount/rowCount.</param>
    /// <param name="registry">The column reader registry.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="columnCount">The pre-read column count.</param>
    /// <param name="rowCount">The pre-read row count.</param>
    /// <param name="protocolVersion">The negotiated protocol version.</param>
    /// <returns>The parsed typed block.</returns>
    public static TypedBlock ReadColumnsWithHeader(
        ref ProtocolReader reader,
        ColumnReaderRegistry registry,
        string tableName,
        int columnCount,
        int rowCount,
        int protocolVersion = 0)
    {
        if (columnCount == 0)
        {
            return new TypedBlock
            {
                TableName = tableName,
                ColumnNames = Array.Empty<string>(),
                ColumnTypes = Array.Empty<string>(),
                Columns = Array.Empty<ITypedColumn>()
            };
        }

        var columnNames = new string[columnCount];
        var columnTypes = new string[columnCount];
        var columns = new ITypedColumn[columnCount];
        var parsedColumnCount = 0;

        try
        {
            for (int i = 0; i < columnCount; i++)
            {
                columnNames[i] = reader.ReadString();
                columnTypes[i] = reader.ReadString();

                // Skip custom serialization metadata for protocol >= WithCustomSerialization
                if (protocolVersion >= ProtocolVersion.WithCustomSerialization)
                {
                    var hasCustom = reader.ReadByte();
                    if (hasCustom != 0)
                    {
                        // Custom serialization kind - skip it
                        reader.ReadByte();
                    }
                }

                if (rowCount > 0)
                {
                    var columnReader = registry.GetReader(columnTypes[i]);
                    columns[i] = columnReader.ReadTypedColumn(ref reader, rowCount);
                }
                else
                {
                    columns[i] = new TypedColumn<object?>(Array.Empty<object?>());
                }
                parsedColumnCount = i + 1;
            }

            return new TypedBlock
            {
                TableName = tableName,
                ColumnNames = columnNames,
                ColumnTypes = columnTypes,
                Columns = columns
            };
        }
        catch
        {
            // On parse failure (e.g., insufficient data), dispose all successfully parsed columns
            // to return their ArrayPool arrays and avoid memory waste
            for (int i = 0; i < parsedColumnCount; i++)
            {
                columns[i]?.Dispose();
            }
            throw;
        }
    }

    /// <summary>
    /// Reads a typed block from the protocol reader using a pre-read table name.
    /// This is the fast path that avoids boxing for value types.
    /// </summary>
    /// <param name="reader">The protocol reader positioned after the table name.</param>
    /// <param name="registry">The column reader registry.</param>
    /// <param name="tableName">The table name that was read before decompression.</param>
    /// <param name="protocolVersion">The negotiated protocol version.</param>
    /// <returns>The parsed typed block.</returns>
    public static TypedBlock ReadTypedBlockWithTableName(ref ProtocolReader reader, ColumnReaderRegistry registry, string tableName, int protocolVersion = 0)
    {
        var info = BlockInfo.Read(ref reader);
        var columnCount = (int)reader.ReadVarInt();
        var rowCount = (int)reader.ReadVarInt();

        if (columnCount == 0)
        {
            return new TypedBlock
            {
                TableName = tableName,
                ColumnNames = Array.Empty<string>(),
                ColumnTypes = Array.Empty<string>(),
                Columns = Array.Empty<ITypedColumn>()
            };
        }

        var columnNames = new string[columnCount];
        var columnTypes = new string[columnCount];
        var columns = new ITypedColumn[columnCount];
        var parsedColumnCount = 0;

        try
        {
            for (int i = 0; i < columnCount; i++)
            {
                columnNames[i] = reader.ReadString();
                columnTypes[i] = reader.ReadString();

                // Skip custom serialization metadata for protocol >= WithCustomSerialization
                if (protocolVersion >= ProtocolVersion.WithCustomSerialization)
                {
                    var hasCustom = reader.ReadByte();
                    if (hasCustom != 0)
                    {
                        // Custom serialization kind - skip it
                        reader.ReadByte();
                    }
                }

                if (rowCount > 0)
                {
                    var columnReader = registry.GetReader(columnTypes[i]);
                    columns[i] = columnReader.ReadTypedColumn(ref reader, rowCount);
                }
                else
                {
                    columns[i] = new TypedColumn<object?>(Array.Empty<object?>());
                }
                parsedColumnCount = i + 1;
            }

            return new TypedBlock
            {
                TableName = tableName,
                ColumnNames = columnNames,
                ColumnTypes = columnTypes,
                Columns = columns
            };
        }
        catch
        {
            // On parse failure (e.g., insufficient data), dispose all successfully parsed columns
            // to return their ArrayPool arrays and avoid memory waste
            for (int i = 0; i < parsedColumnCount; i++)
            {
                columns[i]?.Dispose();
            }
            throw;
        }
    }

    /// <summary>
    /// Writes an empty block to the protocol writer (for query execution).
    /// </summary>
    /// <param name="writer">The protocol writer.</param>
    public static void WriteEmpty(ref ProtocolWriter writer)
    {
        // Message type for client data
        writer.WriteVarInt((ulong)Protocol.ClientMessageType.Data);

        // Empty table name
        writer.WriteString(string.Empty);

        // Block info
        BlockInfo.Default.Write(ref writer);

        // Column count = 0
        writer.WriteVarInt(0);

        // Row count = 0
        writer.WriteVarInt(0);
    }

    /// <summary>
    /// Writes an empty block to the protocol writer with optional compression.
    /// </summary>
    /// <param name="writer">The protocol writer.</param>
    /// <param name="compressor">The compressor to use, or null for no compression.</param>
    public static void WriteEmpty(ref ProtocolWriter writer, ICompressor? compressor)
    {
        // Write message type (not compressed)
        writer.WriteVarInt((ulong)Protocol.ClientMessageType.Data);

        // Table name is ALWAYS written outside the compressed block
        writer.WriteString(string.Empty);

        if (compressor == null)
        {
            // No compression - write block data directly
            WriteBlockDataWithoutTableName(ref writer, BlockInfo.Default, 0, 0);
        }
        else
        {
            // Compress only the block data (table name is already written outside)
            var uncompressedBuffer = new ArrayBufferWriter<byte>();
            var tempWriter = new ProtocolWriter(uncompressedBuffer);
            WriteBlockDataWithoutTableName(ref tempWriter, BlockInfo.Default, 0, 0);

            // Compress and write (using pooled buffers to reduce GC pressure)
            using var compressed = CompressedBlock.CompressPooled(uncompressedBuffer.WrittenSpan, compressor);
            writer.WriteBytes(compressed.Span);
        }
    }

    /// <summary>
    /// Writes block data (without message type) to the protocol writer.
    /// </summary>
    private static void WriteBlockData(ref ProtocolWriter writer, string tableName, BlockInfo info, int columnCount, int rowCount)
    {
        writer.WriteString(tableName);
        WriteBlockDataWithoutTableName(ref writer, info, columnCount, rowCount);
    }

    /// <summary>
    /// Writes block data without the table name (for compression where table name is separate).
    /// </summary>
    private static void WriteBlockDataWithoutTableName(ref ProtocolWriter writer, BlockInfo info, int columnCount, int rowCount)
    {
        info.Write(ref writer);
        writer.WriteVarInt((ulong)columnCount);
        writer.WriteVarInt((ulong)rowCount);
    }

    /// <summary>
    /// Writes a data block with column data to the protocol writer (without message type).
    /// </summary>
    /// <param name="writer">The protocol writer.</param>
    /// <param name="columnNames">Column names.</param>
    /// <param name="columnTypes">ClickHouse column type names.</param>
    /// <param name="columnData">Column data arrays.</param>
    /// <param name="rowCount">Number of rows.</param>
    /// <param name="writerRegistry">Column writer registry.</param>
    /// <param name="protocolVersion">Negotiated protocol version.</param>
    public static void WriteData(
        ref ProtocolWriter writer,
        string[] columnNames,
        string[] columnTypes,
        object?[][] columnData,
        int rowCount,
        ColumnWriterRegistry writerRegistry,
        int protocolVersion)
    {
        // NOTE: Table name is now written by the caller (SendDataBlockAsync) at the Data message level,
        // matching clickhouse-go's structure where sendData() writes table name outside block.Encode()

        // Block info
        BlockInfo.Default.Write(ref writer);

        // Column count and row count
        writer.WriteVarInt((ulong)columnNames.Length);
        writer.WriteVarInt((ulong)rowCount);

        // Write each column
        for (int i = 0; i < columnNames.Length; i++)
        {
            writer.WriteString(columnNames[i]);
            writer.WriteString(columnTypes[i]);

            // Custom serialization byte: server expects this for protocol >= 54454
            // Write 0 to indicate "no custom serialization"
            if (protocolVersion >= Protocol.ProtocolVersion.WithCustomSerialization)
            {
                writer.WriteByte(0); // hasCustom = false
            }

            // Write column data
            if (rowCount > 0)
            {
                var columnWriter = writerRegistry.GetWriter(columnTypes[i]);
                var data = columnData[i].Length == rowCount
                    ? columnData[i]
                    : columnData[i][..rowCount];
                columnWriter.WriteColumn(ref writer, data);
            }
        }
    }

    /// <summary>
    /// Writes a data block with column data and compression to the protocol writer.
    /// Table name is written outside compression, block data is compressed.
    /// </summary>
    /// <param name="writer">The protocol writer.</param>
    /// <param name="columnNames">Column names.</param>
    /// <param name="columnTypes">ClickHouse column type names.</param>
    /// <param name="columnData">Column data arrays.</param>
    /// <param name="rowCount">Number of rows.</param>
    /// <param name="writerRegistry">Column writer registry.</param>
    /// <param name="protocolVersion">Negotiated protocol version.</param>
    /// <param name="compressor">The compressor to use.</param>
    public static void WriteDataCompressed(
        ref ProtocolWriter writer,
        string[] columnNames,
        string[] columnTypes,
        object?[][] columnData,
        int rowCount,
        ColumnWriterRegistry writerRegistry,
        int protocolVersion,
        ICompressor compressor)
    {
        // NOTE: Table name is now written by the caller (SendDataBlockAsync) at the Data message level,
        // matching clickhouse-go's structure where sendData() writes table name outside block.Encode()

        // Build uncompressed block data
        var uncompressedBuffer = new ArrayBufferWriter<byte>();
        var tempWriter = new ProtocolWriter(uncompressedBuffer);

        // Write block info
        BlockInfo.Default.Write(ref tempWriter);

        // Column count and row count
        tempWriter.WriteVarInt((ulong)columnNames.Length);
        tempWriter.WriteVarInt((ulong)rowCount);

        // Write each column
        for (int i = 0; i < columnNames.Length; i++)
        {
            tempWriter.WriteString(columnNames[i]);
            tempWriter.WriteString(columnTypes[i]);

            // Custom serialization byte: server expects this for protocol >= 54454
            // Write 0 to indicate "no custom serialization"
            if (protocolVersion >= Protocol.ProtocolVersion.WithCustomSerialization)
            {
                tempWriter.WriteByte(0); // hasCustom = false
            }

            // Write column data
            if (rowCount > 0)
            {
                var columnWriter = writerRegistry.GetWriter(columnTypes[i]);
                var data = columnData[i].Length == rowCount
                    ? columnData[i]
                    : columnData[i][..rowCount];
                columnWriter.WriteColumn(ref tempWriter, data);
            }
        }

        // Compress and write (using pooled buffers to reduce GC pressure)
        using var compressed = CompressedBlock.CompressPooled(uncompressedBuffer.WrittenSpan, compressor);
        writer.WriteBytes(compressed.Span);
    }
}
