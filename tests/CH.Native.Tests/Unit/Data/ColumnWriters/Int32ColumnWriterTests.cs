using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class Int32ColumnWriterTests
{
    [Fact]
    public void TypeName_IsInt32() => Assert.Equal("Int32", new Int32ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsInt() => Assert.Equal(typeof(int), new Int32ColumnWriter().ClrType);

    [Theory]
    [InlineData(0)]
    [InlineData(int.MinValue)]
    [InlineData(int.MaxValue)]
    [InlineData(-1)]
    [InlineData(0x12345678)]
    public void WriteValue_LittleEndian(int value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int32ColumnWriter().WriteValue(ref writer, value);

        Assert.Equal(4, buffer.WrittenCount);
        Assert.Equal(value, BinaryPrimitives.ReadInt32LittleEndian(buffer.WrittenSpan));
    }

    [Fact]
    public void WriteColumn_RoundTripsThroughReader()
    {
        var values = new[] { int.MinValue, -1, 0, 1, 12345, int.MaxValue };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int32ColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new Int32ColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(values.Length, column.Count);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void NonGeneric_WriteValue_AcceptsBoxedInt()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        ((IColumnWriter)new Int32ColumnWriter()).WriteValue(ref writer, 42);

        Assert.Equal(42, BinaryPrimitives.ReadInt32LittleEndian(buffer.WrittenSpan));
    }

    [Fact]
    public void Registry_ResolvesInt32Writer()
    {
        Assert.IsType<Int32ColumnWriter>(ColumnWriterRegistry.Default.GetWriter("Int32"));
    }
}
