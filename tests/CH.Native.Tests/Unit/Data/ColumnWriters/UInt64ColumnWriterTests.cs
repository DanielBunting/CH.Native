using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class UInt64ColumnWriterTests
{
    [Fact]
    public void TypeName_IsUInt64() => Assert.Equal("UInt64", new UInt64ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsULong() => Assert.Equal(typeof(ulong), new UInt64ColumnWriter().ClrType);

    [Theory]
    [InlineData(0ul)]
    [InlineData(1ul)]
    [InlineData(ulong.MaxValue)]
    [InlineData(9223372036854775808ul)] // would overflow signed long
    public void WriteValue_LittleEndian(ulong value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new UInt64ColumnWriter().WriteValue(ref writer, value);

        Assert.Equal(8, buffer.WrittenCount);
        Assert.Equal(value, BinaryPrimitives.ReadUInt64LittleEndian(buffer.WrittenSpan));
    }

    [Fact]
    public void WriteColumn_RoundTripsThroughReader()
    {
        var values = new ulong[] { 0ul, 1ul, long.MaxValue, 9223372036854775808ul, ulong.MaxValue };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new UInt64ColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new UInt64ColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(values.Length, column.Count);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void Registry_ResolvesUInt64Writer()
    {
        Assert.IsType<UInt64ColumnWriter>(ColumnWriterRegistry.Default.GetWriter("UInt64"));
    }
}
