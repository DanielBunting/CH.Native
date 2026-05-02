using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class Enum8ColumnWriterTests
{
    [Fact]
    public void TypeName_IsEnum8() => Assert.Equal("Enum8", new Enum8ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsSByte() => Assert.Equal(typeof(sbyte), new Enum8ColumnWriter().ClrType);

    [Theory]
    [InlineData((sbyte)0, (byte)0x00)]
    [InlineData((sbyte)1, (byte)0x01)]
    [InlineData(sbyte.MinValue, (byte)0x80)]
    [InlineData(sbyte.MaxValue, (byte)0x7F)]
    public void WriteValue_EmitsTwosComplementByte(sbyte value, byte expected)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Enum8ColumnWriter().WriteValue(ref writer, value);

        Assert.Equal(1, buffer.WrittenCount);
        Assert.Equal(expected, buffer.WrittenSpan[0]);
    }

    [Fact]
    public void WriteColumn_PackedSbytes()
    {
        var values = new sbyte[] { 1, 2, 3, -4 };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Enum8ColumnWriter().WriteColumn(ref writer, values);

        Assert.Equal(4, buffer.WrittenCount);
        Assert.Equal(0x01, buffer.WrittenSpan[0]);
        Assert.Equal(0x02, buffer.WrittenSpan[1]);
        Assert.Equal(0x03, buffer.WrittenSpan[2]);
        Assert.Equal(0xFC, buffer.WrittenSpan[3]); // -4 as two's complement
    }
}
