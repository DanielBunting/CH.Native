using System.Net;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for IPv6 values.
/// IPv6 in ClickHouse is stored as 16 bytes.
/// </summary>
public sealed class IPv6ColumnReader : IColumnReader<IPAddress>
{
    /// <inheritdoc />
    public string TypeName => "IPv6";

    /// <inheritdoc />
    public Type ClrType => typeof(IPAddress);

    /// <inheritdoc />
    public IPAddress ReadValue(ref ProtocolReader reader)
    {
        // IPv6 is stored as 16 bytes in network byte order
        var bytes = reader.ReadBytes(16);
        return new IPAddress(bytes.Span);
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
