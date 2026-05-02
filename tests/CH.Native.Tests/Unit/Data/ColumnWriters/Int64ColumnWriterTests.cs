using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class Int64ColumnWriterTests
{
    [Fact]
    public void TypeName_IsInt64() => Assert.Equal("Int64", new Int64ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsLong() => Assert.Equal(typeof(long), new Int64ColumnWriter().ClrType);

    [Theory]
    [InlineData(0L)]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    [InlineData(-1L)]
    [InlineData(0x123456789ABCDEF0L)]
    public void WriteValue_LittleEndian(long value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int64ColumnWriter().WriteValue(ref writer, value);

        Assert.Equal(8, buffer.WrittenCount);
        Assert.Equal(value, BinaryPrimitives.ReadInt64LittleEndian(buffer.WrittenSpan));
    }

    [Fact]
    public void WriteColumn_RoundTripsThroughReader()
    {
        var values = new[] { long.MinValue, -1L, 0L, 1L, long.MaxValue };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int64ColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new Int64ColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(values.Length, column.Count);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void Registry_ResolvesInt64Writer()
    {
        Assert.IsType<Int64ColumnWriter>(ColumnWriterRegistry.Default.GetWriter("Int64"));
    }
}
