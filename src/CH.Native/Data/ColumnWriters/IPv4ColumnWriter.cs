using System.Net;
using System.Net.Sockets;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for IPv4 values.
/// IPv4 in ClickHouse is stored as 4 bytes in little-endian order.
/// </summary>
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
            WriteValue(ref writer, values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, IPAddress value)
    {
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
            WriteValue(ref writer, (IPAddress)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        WriteValue(ref writer, (IPAddress)value!);
    }
}
