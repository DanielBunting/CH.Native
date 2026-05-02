using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class UInt16ColumnWriterTests
{
    [Fact]
    public void TypeName_IsUInt16() => Assert.Equal("UInt16", new UInt16ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsUShort() => Assert.Equal(typeof(ushort), new UInt16ColumnWriter().ClrType);

    [Theory]
    [InlineData((ushort)0)]
    [InlineData((ushort)1)]
    [InlineData(ushort.MaxValue)]
    [InlineData((ushort)32768)] // would overflow signed short
    public void WriteValue_LittleEndian(ushort value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new UInt16ColumnWriter().WriteValue(ref writer, value);

        Assert.Equal(2, buffer.WrittenCount);
        Assert.Equal(value, BinaryPrimitives.ReadUInt16LittleEndian(buffer.WrittenSpan));
    }

    [Fact]
    public void WriteColumn_RoundTripsThroughReader()
    {
        var values = new ushort[] { 0, 1, 32767, 32768, ushort.MaxValue };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new UInt16ColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new UInt16ColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(values.Length, column.Count);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void Registry_ResolvesUInt16Writer()
    {
        Assert.IsType<UInt16ColumnWriter>(ColumnWriterRegistry.Default.GetWriter("UInt16"));
    }
}
