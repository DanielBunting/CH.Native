using System.Net;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for IPv4 values.
/// IPv4 in ClickHouse is stored as UInt32 in little-endian byte order.
/// </summary>
public sealed class IPv4ColumnReader : IColumnReader<IPAddress>
{
    /// <inheritdoc />
    public string TypeName => "IPv4";

    /// <inheritdoc />
    public Type ClrType => typeof(IPAddress);

    /// <inheritdoc />
    public IPAddress ReadValue(ref ProtocolReader reader)
    {
        // IPv4 is stored as 4 bytes in network byte order (big-endian)
        // but ClickHouse stores it in little-endian, so we need to reverse
        var bytes = reader.ReadBytes(4);
        Span<byte> reversed = stackalloc byte[4];
        reversed[0] = bytes.Span[3];
        reversed[1] = bytes.Span[2];
        reversed[2] = bytes.Span[1];
        reversed[3] = bytes.Span[0];
        return new IPAddress(reversed);
    }

    /// <inheritdoc />
    public TypedColumn<IPAddress> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var values = new IPAddress[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = ReadValue(ref reader);
        }
        return new TypedColumn<IPAddress>(values);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
