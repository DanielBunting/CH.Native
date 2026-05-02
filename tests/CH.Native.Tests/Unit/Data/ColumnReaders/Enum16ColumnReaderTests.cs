using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class Enum16ColumnReaderTests
{
    [Fact]
    public void TypeName_IsEnum16() => Assert.Equal("Enum16", new Enum16ColumnReader().TypeName);

    [Fact]
    public void ClrType_IsShort() => Assert.Equal(typeof(short), new Enum16ColumnReader().ClrType);

    [Theory]
    [InlineData((short)0)]
    [InlineData(short.MinValue)]
    [InlineData(short.MaxValue)]
    [InlineData((short)1000)]
    [InlineData((short)-1)]
    public void ReadValue_LittleEndian(short value)
    {
        var bytes = new byte[2];
        BinaryPrimitives.WriteInt16LittleEndian(bytes, value);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var result = new Enum16ColumnReader().ReadValue(ref reader);

        Assert.Equal(value, result);
    }

    [Fact]
    public void ReadColumn_StridesByTwoBytes()
    {
        var values = new short[] { 1000, 2000, -1000, short.MaxValue };
        var bytes = new byte[values.Length * 2];
        for (int i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt16LittleEndian(bytes.AsSpan(i * 2), values[i]);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = new Enum16ColumnReader().ReadTypedColumn(ref reader, values.Length);

        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], column[i]);
    }
}
