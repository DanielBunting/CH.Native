using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class Enum8ColumnReaderTests
{
    [Fact]
    public void TypeName_IsEnum8() => Assert.Equal("Enum8", new Enum8ColumnReader().TypeName);

    [Fact]
    public void ClrType_IsSByte() => Assert.Equal(typeof(sbyte), new Enum8ColumnReader().ClrType);

    [Theory]
    [InlineData((byte)0x00, (sbyte)0)]
    [InlineData((byte)0x7F, sbyte.MaxValue)]
    [InlineData((byte)0x80, sbyte.MinValue)]
    [InlineData((byte)0xFF, (sbyte)-1)]
    public void ReadValue_DecodesTwosComplement(byte wire, sbyte expected)
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(new[] { wire }));

        var result = new Enum8ColumnReader().ReadValue(ref reader);

        Assert.Equal(expected, result);
    }

    [Fact]
    public void ReadColumn_DecodesEachByte()
    {
        var wire = new byte[] { 0x01, 0x02, 0x80, 0xFC };
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(wire));

        using var column = new Enum8ColumnReader().ReadTypedColumn(ref reader, wire.Length);

        Assert.Equal(4, column.Count);
        Assert.Equal((sbyte)1, column[0]);
        Assert.Equal((sbyte)2, column[1]);
        Assert.Equal(sbyte.MinValue, column[2]);
        Assert.Equal((sbyte)-4, column[3]);
    }
}
