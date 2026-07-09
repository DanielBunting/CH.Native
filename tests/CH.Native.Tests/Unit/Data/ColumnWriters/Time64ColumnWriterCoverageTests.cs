using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class Time64ColumnWriterCoverageTests
{
    [Fact]
    public void Precision_And_ClrType()
    {
        var w = new Time64ColumnWriter(6);
        Assert.Equal(6, w.Precision);
        Assert.Equal(typeof(TimeOnly), w.ClrType);
    }

    [Theory]
    [InlineData(9, 100L)]   // ticks * 10^(9-7)
    [InlineData(8, 10L)]    // ticks * 10^(8-7)
    public void WriteValue_PrecisionAbove7_MultipliesTicks(int precision, long multiplier)
    {
        var time = new TimeOnly(0, 0, 1); // 10_000_000 ticks
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        new Time64ColumnWriter(precision).WriteValue(ref pw, time);
        Assert.Equal(8, buffer.WrittenCount);
        Assert.Equal(time.Ticks * multiplier, BinaryPrimitives.ReadInt64LittleEndian(buffer.WrittenSpan));
    }

    [Fact]
    public void WriteColumn_WritesEach()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        new Time64ColumnWriter(3).WriteColumn(ref pw, new[] { new TimeOnly(0, 0, 1), new TimeOnly(0, 0, 2) });
        Assert.Equal(16, buffer.WrittenCount);
    }

    [Fact]
    public void IColumnWriter_WriteValue_And_WriteColumn()
    {
        IColumnWriter w = new Time64ColumnWriter(3);

        var b1 = new ArrayBufferWriter<byte>();
        var p1 = new ProtocolWriter(b1);
        w.WriteValue(ref p1, new TimeOnly(0, 0, 1));
        Assert.Equal(1000L, BinaryPrimitives.ReadInt64LittleEndian(b1.WrittenSpan)); // 1s at ms precision

        var b2 = new ArrayBufferWriter<byte>();
        var p2 = new ProtocolWriter(b2);
        w.WriteColumn(ref p2, new object?[] { new TimeOnly(0, 0, 1) });
        Assert.Equal(8, b2.WrittenCount);
    }
}
