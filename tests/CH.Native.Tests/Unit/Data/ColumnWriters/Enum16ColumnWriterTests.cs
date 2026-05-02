using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class Enum16ColumnWriterTests
{
    [Fact]
    public void TypeName_IsEnum16() => Assert.Equal("Enum16", new Enum16ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsShort() => Assert.Equal(typeof(short), new Enum16ColumnWriter().ClrType);

    [Theory]
    [InlineData((short)0)]
    [InlineData((short)1000)]
    [InlineData(short.MinValue)]
    [InlineData(short.MaxValue)]
    public void WriteValue_LittleEndianTwoBytes(short value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Enum16ColumnWriter().WriteValue(ref writer, value);

        Assert.Equal(2, buffer.WrittenCount);
        Assert.Equal(value, BinaryPrimitives.ReadInt16LittleEndian(buffer.WrittenSpan));
    }

    [Fact]
    public void WriteColumn_LargeBlock_StridesCorrectly()
    {
        var values = new short[] { 1000, 2000, 3000, -1000 };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Enum16ColumnWriter().WriteColumn(ref writer, values);

        Assert.Equal(values.Length * 2, buffer.WrittenCount);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], BinaryPrimitives.ReadInt16LittleEndian(buffer.WrittenSpan.Slice(i * 2, 2)));
    }
}
