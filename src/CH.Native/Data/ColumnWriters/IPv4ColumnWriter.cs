using System.Net;
using System.Net.Sockets;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for IPv4 values.
/// </summary>
/// <remarks>
/// ClickHouse stores IPv4 as a 4-byte little-endian <see cref="uint"/> on the
/// wire. <see cref="IPAddress.GetAddressBytes()"/> returns bytes in
/// <em>network order</em> (big-endian), so the writer reverses them before
/// emitting. <c>1.2.3.4</c> goes out on the wire as
/// <c>[0x04, 0x03, 0x02, 0x01]</c> — symmetric with <see cref="IPv4ColumnReader"/>.
/// </remarks>
public sealed class IPv4ColumnWriter : IColumnWriter<IPAddress>
{
    /// <inheritdoc />
    public string TypeName => "IPv4";

    /// <inheritdoc />
    public Type ClrType => typeof(IPAddress);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, IPAddress[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is null)
                throw NullAt(i);
            WriteValue(ref writer, values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, IPAddress value)
    {
        if (value is null)
            throw NullAt(rowIndex: -1);

        // IPAddress bytes are in network byte order (big-endian)
        // ClickHouse stores IPv4 in little-endian, so we need to reverse
        byte[] ipBytes;

        if (value.AddressFamily == AddressFamily.InterNetwork)
        {
            ipBytes = value.GetAddressBytes();
        }
        else if (value.IsIPv4MappedToIPv6)
        {
            // Extract IPv4 from mapped IPv6
            ipBytes = value.MapToIPv4().GetAddressBytes();
        }
        else
        {
            // Default to 0.0.0.0 for invalid addresses
            ipBytes = new byte[4];
        }

        // Reverse to little-endian for ClickHouse
        writer.WriteByte(ipBytes[3]);
        writer.WriteByte(ipBytes[2]);
        writer.WriteByte(ipBytes[1]);
        writer.WriteByte(ipBytes[0]);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is null)
                throw NullAt(i);
            WriteValue(ref writer, (IPAddress)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        if (value is null)
            throw NullAt(rowIndex: -1);
        WriteValue(ref writer, (IPAddress)value);
    }

    private static InvalidOperationException NullAt(int rowIndex)
    {
        var where = rowIndex >= 0 ? $" at row {rowIndex}" : string.Empty;
        return new InvalidOperationException(
            $"IPv4ColumnWriter received null{where}. The IPv4 column type is non-nullable; " +
            $"declare the column as Nullable(IPv4) and wrap this writer with NullableRefColumnWriter, " +
            $"or ensure source values are non-null.");
    }
}
