using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class Int16ColumnWriterTests
{
    [Fact]
    public void TypeName_IsInt16() => Assert.Equal("Int16", new Int16ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsShort() => Assert.Equal(typeof(short), new Int16ColumnWriter().ClrType);

    [Theory]
    [InlineData((short)0, new byte[] { 0x00, 0x00 })]
    [InlineData(short.MinValue, new byte[] { 0x00, 0x80 })]
    [InlineData(short.MaxValue, new byte[] { 0xFF, 0x7F })]
    [InlineData((short)-1, new byte[] { 0xFF, 0xFF })]
    [InlineData((short)256, new byte[] { 0x00, 0x01 })]
    public void WriteValue_LittleEndian(short value, byte[] expected)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int16ColumnWriter().WriteValue(ref writer, value);

        Assert.Equal(2, buffer.WrittenCount);
        Assert.Equal(expected, buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteColumn_RoundTripsThroughReader()
    {
        var values = new short[] { short.MinValue, -1, 0, 1, 256, short.MaxValue };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int16ColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new Int16ColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(values.Length, column.Count);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void WriteColumn_LargeBlock_PreservesStride()
    {
        var values = new short[1024];
        for (int i = 0; i < values.Length; i++) values[i] = (short)(i - 512);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int16ColumnWriter().WriteColumn(ref writer, values);

        Assert.Equal(values.Length * 2, buffer.WrittenCount);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new Int16ColumnReader().ReadTypedColumn(ref reader, values.Length);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void Registry_ResolvesInt16Writer()
    {
        Assert.IsType<Int16ColumnWriter>(ColumnWriterRegistry.Default.GetWriter("Int16"));
    }
}
