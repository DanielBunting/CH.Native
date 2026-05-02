using System.Buffers;
using System.Net;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class IPv6ColumnReaderTests
{
    [Fact]
    public void TypeName_IsIPv6() => Assert.Equal("IPv6", new IPv6ColumnReader().TypeName);

    [Fact]
    public void ClrType_IsIPAddress() => Assert.Equal(typeof(IPAddress), new IPv6ColumnReader().ClrType);

    [Theory]
    [InlineData("::1")]
    [InlineData("::")]
    [InlineData("2001:db8::1")]
    [InlineData("fe80::1")]
    [InlineData("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")]
    [InlineData("::ffff:192.168.1.1")] // IPv4-mapped
    public void ReadValue_RoundTripsThroughWriter(string address)
    {
        var ip = IPAddress.Parse(address);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new IPv6ColumnWriter().WriteValue(ref writer, ip);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = new IPv6ColumnReader().ReadValue(ref reader);

        Assert.Equal(ip, result);
    }

    [Fact]
    public void ReadValue_NetworkOrderBytes_DecodesUnchanged()
    {
        // IPv6 ::1 is sixteen bytes with only the last set to 0x01.
        var wire = new byte[16];
        wire[15] = 0x01;
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(wire));

        var result = new IPv6ColumnReader().ReadValue(ref reader);

        Assert.Equal(IPAddress.Parse("::1"), result);
    }

    [Fact]
    public void Registry_ResolvesIPv6Reader()
    {
        Assert.IsType<IPv6ColumnReader>(ColumnReaderRegistry.Default.GetReader("IPv6"));
    }
}
