using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class Int8ColumnWriterTests
{
    [Fact]
    public void TypeName_IsInt8() => Assert.Equal("Int8", new Int8ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsSByte() => Assert.Equal(typeof(sbyte), new Int8ColumnWriter().ClrType);

    [Theory]
    [InlineData((sbyte)0, (byte)0x00)]
    [InlineData(sbyte.MinValue, (byte)0x80)]
    [InlineData(sbyte.MaxValue, (byte)0x7F)]
    [InlineData((sbyte)-1, (byte)0xFF)]
    public void WriteValue_EmitsTwosComplementByte(sbyte value, byte expected)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int8ColumnWriter().WriteValue(ref writer, value);

        Assert.Equal(1, buffer.WrittenCount);
        Assert.Equal(expected, buffer.WrittenSpan[0]);
    }

    [Fact]
    public void WriteColumn_EmitsOneBytePerRow()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int8ColumnWriter().WriteColumn(ref writer, new sbyte[] { -128, 0, 127, -1 });

        Assert.Equal(4, buffer.WrittenCount);
        Assert.Equal(0x80, buffer.WrittenSpan[0]);
        Assert.Equal(0x00, buffer.WrittenSpan[1]);
        Assert.Equal(0x7F, buffer.WrittenSpan[2]);
        Assert.Equal(0xFF, buffer.WrittenSpan[3]);
    }

    [Fact]
    public void WriteColumn_RoundTripsThroughReader()
    {
        var values = new sbyte[] { sbyte.MinValue, -1, 0, 1, sbyte.MaxValue };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int8ColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new Int8ColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(values.Length, column.Count);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void Registry_ResolvesInt8Writer()
    {
        Assert.IsType<Int8ColumnWriter>(ColumnWriterRegistry.Default.GetWriter("Int8"));
    }
}
