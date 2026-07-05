using System.Buffers;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Protocol;

/// <summary>
/// Pins the read-past-end contract of <see cref="ProtocolReader"/>: the non-Try primitive
/// readers throw <see cref="InvalidOperationException"/> ("Unexpected end of data…"), not
/// <see cref="EndOfStreamException"/>. The reads live in plain helper methods because a ref
/// struct cannot appear inside a lambda body or as a generic type argument.
/// </summary>
public class ProtocolReaderUnderflowTests
{
    private static void ReadInt16(int available)
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(new byte[available]));
        reader.ReadInt16();
    }

    private static void ReadInt32(int available)
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(new byte[available]));
        reader.ReadInt32();
    }

    private static void ReadInt64(int available)
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(new byte[available]));
        reader.ReadInt64();
    }

    private static void ReadUInt32(int available)
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(new byte[available]));
        reader.ReadUInt32();
    }

    private static void ReadByte(int available)
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(new byte[available]));
        reader.ReadByte();
    }

    [Fact]
    public void ReadInt16_PastEnd_ThrowsInvalidOperation() =>
        Assert.Throws<InvalidOperationException>(() => ReadInt16(1));

    [Fact]
    public void ReadInt32_PastEnd_ThrowsInvalidOperation() =>
        Assert.Throws<InvalidOperationException>(() => ReadInt32(2));

    [Fact]
    public void ReadInt64_PastEnd_ThrowsInvalidOperation() =>
        Assert.Throws<InvalidOperationException>(() => ReadInt64(4));

    [Fact]
    public void ReadUInt32_PastEnd_ThrowsInvalidOperation() =>
        Assert.Throws<InvalidOperationException>(() => ReadUInt32(3));

    [Fact]
    public void ReadByte_EmptyBuffer_ThrowsInvalidOperation() =>
        Assert.Throws<InvalidOperationException>(() => ReadByte(0));
}
