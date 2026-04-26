using System.Buffers;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Protocol;

public class ProtocolWriterTests
{
    [Fact]
    public void WriteByte_WritesCorrectly()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteByte(0x42);

        Assert.Equal([0x42], buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteInt16_WritesLittleEndian()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteInt16(0x0102);

        Assert.Equal([0x02, 0x01], buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteUInt16_WritesLittleEndian()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteUInt16(0x0102);

        Assert.Equal([0x02, 0x01], buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteInt32_WritesLittleEndian()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteInt32(0x01020304);

        Assert.Equal([0x04, 0x03, 0x02, 0x01], buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteUInt32_WritesLittleEndian()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteUInt32(0x01020304);

        Assert.Equal([0x04, 0x03, 0x02, 0x01], buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteInt64_WritesLittleEndian()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteInt64(0x0102030405060708);

        Assert.Equal([0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01], buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteUInt64_WritesLittleEndian()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteUInt64(0x0102030405060708);

        Assert.Equal([0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01], buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteString_WritesLengthPrefixAndUtf8()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteString("Hello");

        // Length prefix (5) + UTF-8 bytes
        Assert.Equal([0x05, 0x48, 0x65, 0x6C, 0x6C, 0x6F], buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteString_EmptyString_WritesZeroLength()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteString("");

        Assert.Equal([0x00], buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteString_Unicode_WritesCorrectUtf8()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteString("\u4e2d\u6587"); // Chinese characters

        // Length prefix (6) + UTF-8 bytes for two CJK characters (3 bytes each)
        var expected = new byte[] { 0x06, 0xE4, 0xB8, 0xAD, 0xE6, 0x96, 0x87 };
        Assert.Equal(expected, buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteBytes_WritesRawBytes()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteBytes([0x01, 0x02, 0x03]);

        Assert.Equal([0x01, 0x02, 0x03], buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteVarInt_WritesCorrectly()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteVarInt(300UL);

        // 300 = 0b100101100 -> [0xAC, 0x02]
        Assert.Equal([0xAC, 0x02], buffer.WrittenSpan.ToArray());
    }

    // Bug #11 in audit 05-protocol-buffers-compression.md:
    // WriteVarInt(int) silently casts a negative int to a huge ulong (e.g. -1 →
    // 0xFFFFFFFFFFFFFFFF), producing a 10-byte encoding that the server will
    // parse as a garbage length/count. VarInt on the wire is always an unsigned
    // length/count in this protocol; a negative argument is always a bug in the
    // caller and should fail fast rather than silently corrupt the wire format.
    //
    // This test encodes the desired contract — reject negatives — so it fails
    // today (the method returns silently) and passes once the cast is guarded.
    [Theory]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void WriteVarInt_NegativeInt_ShouldThrow(int value)
    {
        // ProtocolWriter is a ref struct, so the call can't be inside a lambda.
        // Invoke directly via a helper and assert the exception escapes.
        var buffer = new ArrayBufferWriter<byte>();
        var threw = false;
        try
        {
            var writer = new ProtocolWriter(buffer);
            writer.WriteVarInt(value);
        }
        catch (ArgumentOutOfRangeException)
        {
            threw = true;
        }

        Assert.True(threw,
            $"WriteVarInt({value}) should throw ArgumentOutOfRangeException rather than silently casting to a huge ulong.");
        // And no partial output on the wire.
        Assert.Equal(0, buffer.WrittenCount);
    }
}
