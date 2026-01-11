using System.Buffers;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Protocol;

public class ProtocolReaderTests
{
    [Theory]
    [InlineData(byte.MinValue)]
    [InlineData(byte.MaxValue)]
    [InlineData(42)]
    public void RoundTrip_Byte(byte value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteByte(value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = reader.ReadByte();

        Assert.Equal(value, result);
    }

    [Theory]
    [InlineData(short.MinValue)]
    [InlineData(short.MaxValue)]
    [InlineData(0)]
    [InlineData(12345)]
    public void RoundTrip_Int16(short value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteInt16(value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = reader.ReadInt16();

        Assert.Equal(value, result);
    }

    [Theory]
    [InlineData(ushort.MinValue)]
    [InlineData(ushort.MaxValue)]
    [InlineData(12345)]
    public void RoundTrip_UInt16(ushort value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt16(value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = reader.ReadUInt16();

        Assert.Equal(value, result);
    }

    [Theory]
    [InlineData(int.MinValue)]
    [InlineData(int.MaxValue)]
    [InlineData(0)]
    [InlineData(1234567890)]
    public void RoundTrip_Int32(int value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteInt32(value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = reader.ReadInt32();

        Assert.Equal(value, result);
    }

    [Theory]
    [InlineData(uint.MinValue)]
    [InlineData(uint.MaxValue)]
    [InlineData(1234567890U)]
    public void RoundTrip_UInt32(uint value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt32(value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = reader.ReadUInt32();

        Assert.Equal(value, result);
    }

    [Theory]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    [InlineData(0)]
    [InlineData(1234567890123456789L)]
    public void RoundTrip_Int64(long value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteInt64(value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = reader.ReadInt64();

        Assert.Equal(value, result);
    }

    [Theory]
    [InlineData(ulong.MinValue)]
    [InlineData(ulong.MaxValue)]
    [InlineData(1234567890123456789UL)]
    public void RoundTrip_UInt64(ulong value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = reader.ReadUInt64();

        Assert.Equal(value, result);
    }

    [Theory]
    [InlineData(0UL)]
    [InlineData(127UL)]
    [InlineData(128UL)]
    [InlineData(16383UL)]
    [InlineData(16384UL)]
    [InlineData(ulong.MaxValue)]
    public void RoundTrip_VarInt(ulong value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteVarInt(value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = reader.ReadVarInt();

        Assert.Equal(value, result);
    }

    [Theory]
    [InlineData("")]
    [InlineData("Hello")]
    [InlineData("Hello, World!")]
    [InlineData("\u4e2d\u6587")] // Chinese characters
    public void RoundTrip_String(string value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteString(value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = reader.ReadString();

        Assert.Equal(value, result);
    }

    [Fact]
    public void RoundTrip_Bytes()
    {
        byte[] data = [0x01, 0x02, 0x03, 0x04, 0x05];

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteBytes(data);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = reader.ReadBytes(data.Length);

        Assert.Equal(data, result.ToArray());
    }

    [Fact]
    public void RoundTrip_MultipleValues()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteVarInt(42UL);
        writer.WriteString("Hello");
        writer.WriteInt32(12345);
        writer.WriteUInt64(ulong.MaxValue);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));

        Assert.Equal(42UL, reader.ReadVarInt());
        Assert.Equal("Hello", reader.ReadString());
        Assert.Equal(12345, reader.ReadInt32());
        Assert.Equal(ulong.MaxValue, reader.ReadUInt64());
    }

    [Fact]
    public void Reader_TracksConsumedAndRemaining()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteInt32(42);
        writer.WriteInt32(43);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));

        Assert.Equal(0, reader.Consumed);
        Assert.Equal(8, reader.Remaining);

        reader.ReadInt32();

        Assert.Equal(4, reader.Consumed);
        Assert.Equal(4, reader.Remaining);
    }
}
