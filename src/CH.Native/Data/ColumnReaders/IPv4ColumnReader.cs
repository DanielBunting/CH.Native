using System.Net;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for IPv4 values.
/// </summary>
/// <remarks>
/// ClickHouse stores IPv4 as a 4-byte little-endian <see cref="uint"/> on the
/// wire. <see cref="IPAddress"/> expects bytes in <em>network order</em>
/// (big-endian), so the reader reverses the wire bytes before constructing
/// the address. For example, <c>1.2.3.4</c> arrives on the wire as
/// <c>[0x04, 0x03, 0x02, 0x01]</c> and is reversed to
/// <c>[0x01, 0x02, 0x03, 0x04]</c> before being passed to
/// <see cref="IPAddress(ReadOnlySpan{byte})"/>.
/// </remarks>
public sealed class IPv4ColumnReader : IColumnReader<IPAddress>
{
    /// <inheritdoc />
    public string TypeName => "IPv4";

    /// <inheritdoc />
    public Type ClrType => typeof(IPAddress);

    /// <inheritdoc />
    public IPAddress ReadValue(ref ProtocolReader reader)
    {
        // Wire bytes are little-endian; reverse to network order for IPAddress.
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
