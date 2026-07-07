using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Numerics;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class Decimal256ColumnWriterTests
{
    private static int WrittenBytes(int scale, ClickHouseDecimal value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        new Decimal256ColumnWriter(scale).WriteValue(ref pw, value);
        return buffer.WrittenCount;
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(77)]
    public void Constructor_ScaleOutOfRange_Throws(int scale) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new Decimal256ColumnWriter(scale));

    [Fact]
    public void TypeName_Scale_ClrType()
    {
        var w = new Decimal256ColumnWriter(10);
        Assert.Equal("Decimal256(10)", w.TypeName);
        Assert.Equal(10, w.Scale);
        Assert.Equal(typeof(ClickHouseDecimal), w.ClrType);
    }

    [Fact]
    public void WriteValue_Rescale_Equal_Up_Down_AllWrite32Bytes()
    {
        // (ClickHouseDecimal)1.23m has scale 2.
        var value = (ClickHouseDecimal)1.23m;
        Assert.Equal(32, WrittenBytes(2, value)); // currentScale == targetScale
        Assert.Equal(32, WrittenBytes(4, value)); // currentScale < targetScale (multiply)
        Assert.Equal(32, WrittenBytes(1, value)); // currentScale > targetScale (divide/truncate)
    }

    [Fact]
    public void WriteColumn_WritesEach()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        new Decimal256ColumnWriter(2).WriteColumn(ref pw, new[] { (ClickHouseDecimal)1m, (ClickHouseDecimal)2m });
        Assert.Equal(64, buffer.WrittenCount);
    }

    [Fact]
    public void IColumnWriter_AcceptsDecimal_And_ClickHouseDecimal_RejectsOther()
    {
        IColumnWriter w = new Decimal256ColumnWriter(2);

        var b1 = new ArrayBufferWriter<byte>();
        var p1 = new ProtocolWriter(b1);
        w.WriteValue(ref p1, (ClickHouseDecimal)1.5m);
        Assert.Equal(32, b1.WrittenCount);

        var b2 = new ArrayBufferWriter<byte>();
        var p2 = new ProtocolWriter(b2);
        w.WriteValue(ref p2, 1.5m); // boxed decimal -> converted
        Assert.Equal(32, b2.WrittenCount);

        Assert.Throws<InvalidCastException>(() =>
        {
            var buf = new ArrayBufferWriter<byte>();
            var pw = new ProtocolWriter(buf);
            w.WriteValue(ref pw, "not a decimal");
        });
    }

    [Fact]
    public void IColumnWriter_WriteColumn_And_NullValue()
    {
        IColumnWriter w = new Decimal256ColumnWriter(2);

        var b = new ArrayBufferWriter<byte>();
        var p = new ProtocolWriter(b);
        w.WriteColumn(ref p, new object?[] { (ClickHouseDecimal)1m, 2m });
        Assert.Equal(64, b.WrittenCount);

        // null -> the "null" arm of the cast error message.
        Assert.Throws<InvalidCastException>(() =>
        {
            var buf = new ArrayBufferWriter<byte>();
            var pw = new ProtocolWriter(buf);
            w.WriteValue(ref pw, null);
        });
    }
}
