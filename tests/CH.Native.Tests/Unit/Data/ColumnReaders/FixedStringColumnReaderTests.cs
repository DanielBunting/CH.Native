using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class FixedStringColumnReaderTests
{
    [Fact]
    public void TypeName_EmbedsLength()
    {
        Assert.Equal("FixedString(8)", new FixedStringColumnReader(8).TypeName);
        Assert.Equal("FixedString(255)", new FixedStringColumnReader(255).TypeName);
    }

    [Fact]
    public void ClrType_IsByteArray() => Assert.Equal(typeof(byte[]), new FixedStringColumnReader(4).ClrType);

    [Fact]
    public void Length_ReflectsConstructor() => Assert.Equal(16, new FixedStringColumnReader(16).Length);

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_NonPositiveLength_Throws(int length)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new FixedStringColumnReader(length));
    }

    [Fact]
    public void ReadValue_ReturnsExactlyNBytes_IncludingNulPadding()
    {
        var wire = new byte[] { (byte)'h', (byte)'i', 0, 0, 0, 0, 0, 0 };
        var reader = new global::CH.Native.Protocol.ProtocolReader(new ReadOnlySequence<byte>(wire));

        var result = new FixedStringColumnReader(8).ReadValue(ref reader);

        Assert.Equal(8, result.Length);
        Assert.Equal(wire, result);
    }

    [Fact]
    public void BytesToString_TrimsTrailingNuls()
    {
        var bytes = new byte[] { (byte)'h', (byte)'i', 0, 0 };
        Assert.Equal("hi", FixedStringColumnReader.BytesToString(bytes));
    }

    [Fact]
    public void BytesToString_NoTrailingNul_ReturnsFullString()
    {
        var bytes = new byte[] { (byte)'a', (byte)'b', (byte)'c', (byte)'d' };
        Assert.Equal("abcd", FixedStringColumnReader.BytesToString(bytes));
    }

    [Fact]
    public void BytesToString_EmbeddedNul_TrimsFromFirstNul()
    {
        // Documented behaviour: the helper finds the first nul, treats the rest
        // as padding. Pin it so a refactor that keeps the embedded nul doesn't
        // silently change consumer-visible string content.
        var bytes = new byte[] { (byte)'a', 0, (byte)'b', (byte)'c' };
        Assert.Equal("a", FixedStringColumnReader.BytesToString(bytes));
    }

    [Fact]
    public void ReadColumn_PerRowStrideEqualsLength()
    {
        var wire = new byte[]
        {
            (byte)'a', (byte)'b', (byte)'c', (byte)'d',
            (byte)'1', (byte)'2', (byte)'3', (byte)'4',
            (byte)'X', (byte)'Y', 0, 0,
        };
        var reader = new global::CH.Native.Protocol.ProtocolReader(new ReadOnlySequence<byte>(wire));

        using var column = new FixedStringColumnReader(4).ReadTypedColumn(ref reader, 3);

        Assert.Equal(3, column.Count);
        Assert.Equal(new byte[] { (byte)'a', (byte)'b', (byte)'c', (byte)'d' }, column[0]);
        Assert.Equal(new byte[] { (byte)'1', (byte)'2', (byte)'3', (byte)'4' }, column[1]);
        Assert.Equal(new byte[] { (byte)'X', (byte)'Y', 0, 0 }, column[2]);
    }
}
