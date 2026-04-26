using System.Net;
using System.Net.Sockets;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for IPv6 values.
/// IPv6 in ClickHouse is stored as 16 bytes in network byte order.
/// </summary>
public sealed class IPv6ColumnWriter : IColumnWriter<IPAddress>
{
    /// <inheritdoc />
    public string TypeName => "IPv6";

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

        byte[] ipBytes;

        if (value.AddressFamily == AddressFamily.InterNetworkV6)
        {
            ipBytes = value.GetAddressBytes();
        }
        else if (value.AddressFamily == AddressFamily.InterNetwork)
        {
            // Map IPv4 to IPv6 format (::ffff:x.x.x.x)
            ipBytes = value.MapToIPv6().GetAddressBytes();
        }
        else
        {
            // Default to :: for invalid addresses
            ipBytes = new byte[16];
        }

        // Write all 16 bytes
        for (int i = 0; i < 16; i++)
        {
            writer.WriteByte(ipBytes[i]);
        }
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
            $"IPv6ColumnWriter received null{where}. The IPv6 column type is non-nullable; " +
            $"declare the column as Nullable(IPv6) and wrap this writer with NullableRefColumnWriter, " +
            $"or ensure source values are non-null.");
    }
}
