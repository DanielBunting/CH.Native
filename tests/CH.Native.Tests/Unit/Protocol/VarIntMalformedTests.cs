using System.Buffers;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Protocol;

/// <summary>
/// Tests for the VarInt 10-byte cap. The LEB128 encoding of a 64-bit integer
/// is at most <see cref="VarInt.MaxLength"/> (10) bytes. A malformed stream
/// with more continuation bytes must be rejected rather than silently
/// producing a corrupt value (C# masks ulong shift counts with &amp; 63, so
/// continuation bytes past position 9 silently OR bits into low positions).
/// </summary>
public class VarIntMalformedTests
{
    // Eleven 0x80 bytes followed by a 0x01 terminator: a stream that exceeds
    // MaxLength but still has a clean terminator. Every decoder should reject
    // this rather than silently decoding it.
    private static byte[] Malformed11ByteStream() => new byte[]
    {
        0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01,
    };

    // A stream with more than MaxLength continuation bytes and no terminator.
    private static byte[] MalformedAllContinuation(int length)
    {
        var buf = new byte[length];
        for (int i = 0; i < length; i++) buf[i] = 0x80;
        return buf;
    }

    [Fact]
    public void VarInt_Read_Over10Bytes_ThrowsInvalidData()
    {
        var stream = Malformed11ByteStream();
        Assert.Throws<InvalidDataException>(() => VarInt.Read(stream, out _));
    }

    [Fact]
    public void ProtocolReader_ReadVarInt_Over10Bytes_ThrowsInvalidData()
    {
        var stream = Malformed11ByteStream();
        var seq = new ReadOnlySequence<byte>(stream);

        Assert.Throws<InvalidDataException>(() =>
        {
            var reader = new ProtocolReader(seq);
            reader.ReadVarInt();
        });
    }

    [Fact]
    public void ProtocolReader_TryReadVarInt_Over10Bytes_ThrowsInvalidData()
    {
        var stream = Malformed11ByteStream();
        var seq = new ReadOnlySequence<byte>(stream);

        Assert.Throws<InvalidDataException>(() =>
        {
            var reader = new ProtocolReader(seq);
            reader.TryReadVarInt(out _);
        });
    }

    [Fact]
    public void ProtocolReader_TrySkipVarInt_Over10Bytes_ThrowsInvalidData()
    {
        var stream = Malformed11ByteStream();
        var seq = new ReadOnlySequence<byte>(stream);

        Assert.Throws<InvalidDataException>(() =>
        {
            var reader = new ProtocolReader(seq);
            reader.TrySkipVarInt();
        });
    }

    // ---- Incomplete stream still returns false / throws end-of-data ------
    // (malformed ≠ incomplete; the existing semantics for short reads are
    // preserved.)

    [Fact]
    public void ProtocolReader_TryReadVarInt_IncompleteStream_ReturnsFalse()
    {
        // Fewer than MaxLength continuation bytes with no terminator and no
        // more data available — this is "not enough data," not malformed.
        var stream = MalformedAllContinuation(5);
        var seq = new ReadOnlySequence<byte>(stream);

        var reader = new ProtocolReader(seq);
        var ok = reader.TryReadVarInt(out _);

        Assert.False(ok);
    }

    [Fact]
    public void ProtocolReader_TrySkipVarInt_IncompleteStream_ReturnsFalse()
    {
        var stream = MalformedAllContinuation(5);
        var seq = new ReadOnlySequence<byte>(stream);

        var reader = new ProtocolReader(seq);
        var ok = reader.TrySkipVarInt();

        Assert.False(ok);
    }

    // ---- Valid 10-byte maximum still decodes correctly -------------------

    [Fact]
    public void VarInt_Read_ValidMaxEncoding_DecodesUlongMaxValue()
    {
        // ulong.MaxValue encodes to exactly 10 bytes: 0xFF × 9, then 0x01.
        Span<byte> buffer = stackalloc byte[VarInt.MaxLength];
        int written = VarInt.Write(buffer, ulong.MaxValue);
        Assert.Equal(10, written);

        var result = VarInt.Read(buffer, out int bytesRead);
        Assert.Equal(ulong.MaxValue, result);
        Assert.Equal(10, bytesRead);
    }

    [Fact]
    public void ProtocolReader_ReadVarInt_ValidMaxEncoding_DecodesUlongMaxValue()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        writer.WriteVarInt(ulong.MaxValue);
        var seq = new ReadOnlySequence<byte>(buf.WrittenMemory);

        var reader = new ProtocolReader(seq);
        var result = reader.ReadVarInt();

        Assert.Equal(ulong.MaxValue, result);
    }
}
