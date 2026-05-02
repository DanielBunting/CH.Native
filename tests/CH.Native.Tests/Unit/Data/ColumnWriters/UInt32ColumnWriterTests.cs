using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class UInt32ColumnWriterTests
{
    [Fact]
    public void TypeName_IsUInt32() => Assert.Equal("UInt32", new UInt32ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsUInt() => Assert.Equal(typeof(uint), new UInt32ColumnWriter().ClrType);

    [Theory]
    [InlineData(0u)]
    [InlineData(1u)]
    [InlineData(uint.MaxValue)]
    [InlineData(2147483648u)] // would overflow signed int
    public void WriteValue_LittleEndian(uint value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new UInt32ColumnWriter().WriteValue(ref writer, value);

        Assert.Equal(4, buffer.WrittenCount);
        Assert.Equal(value, BinaryPrimitives.ReadUInt32LittleEndian(buffer.WrittenSpan));
    }

    [Fact]
    public void WriteColumn_RoundTripsThroughReader()
    {
        var values = new uint[] { 0u, 1u, int.MaxValue, 2147483648u, uint.MaxValue };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new UInt32ColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new UInt32ColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(values.Length, column.Count);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void Registry_ResolvesUInt32Writer()
    {
        Assert.IsType<UInt32ColumnWriter>(ColumnWriterRegistry.Default.GetWriter("UInt32"));
    }
}
