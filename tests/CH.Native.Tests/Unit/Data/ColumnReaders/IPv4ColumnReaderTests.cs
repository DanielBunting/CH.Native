using System.Buffers;
using System.Net;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// ClickHouse stores IPv4 as a little-endian UInt32 — i.e. <c>1.2.3.4</c>
/// arrives on the wire as <c>[0x04, 0x03, 0x02, 0x01]</c>. The reader must
/// reverse to network order before constructing the IPAddress. Pin that.
/// </summary>
public class IPv4ColumnReaderTests
{
    [Fact]
    public void TypeName_IsIPv4() => Assert.Equal("IPv4", new IPv4ColumnReader().TypeName);

    [Fact]
    public void ClrType_IsIPAddress() => Assert.Equal(typeof(IPAddress), new IPv4ColumnReader().ClrType);

    [Fact]
    public void ReadValue_KnownLittleEndianBytes_DecodesToNetworkOrder()
    {
        // Wire layout for 1.2.3.4 is little-endian: [0x04, 0x03, 0x02, 0x01]
        var wire = new byte[] { 0x04, 0x03, 0x02, 0x01 };
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(wire));

        var result = new IPv4ColumnReader().ReadValue(ref reader);

        Assert.Equal(IPAddress.Parse("1.2.3.4"), result);
    }

    [Theory]
    [InlineData("0.0.0.0")]
    [InlineData("127.0.0.1")]
    [InlineData("255.255.255.255")]
    [InlineData("192.168.1.1")]
    [InlineData("10.0.0.1")]
    public void ReadValue_RoundTripsThroughWriter(string address)
    {
        var ip = IPAddress.Parse(address);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new IPv4ColumnWriter().WriteValue(ref writer, ip);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = new IPv4ColumnReader().ReadValue(ref reader);

        Assert.Equal(ip, result);
    }

    [Fact]
    public void Registry_ResolvesIPv4Reader()
    {
        Assert.IsType<IPv4ColumnReader>(ColumnReaderRegistry.Default.GetReader("IPv4"));
    }
}
