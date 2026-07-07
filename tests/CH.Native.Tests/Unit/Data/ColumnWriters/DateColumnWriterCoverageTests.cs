using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class DateColumnWriterCoverageTests
{
    [Fact]
    public void TypeName_And_ClrType()
    {
        var w = new DateColumnWriter();
        Assert.Equal("Date", w.TypeName);
        Assert.Equal(typeof(DateOnly), w.ClrType);
    }

    [Fact]
    public void WriteValue_EncodesDaysSinceEpoch()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        new DateColumnWriter().WriteValue(ref pw, new DateOnly(1970, 1, 11)); // 10 days
        Assert.Equal(2, buffer.WrittenCount);
        Assert.Equal(10, BinaryPrimitives.ReadUInt16LittleEndian(buffer.WrittenSpan));
    }

    [Fact]
    public void WriteColumn_WritesEach()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        new DateColumnWriter().WriteColumn(ref pw, new[] { new DateOnly(1970, 1, 1), new DateOnly(1970, 1, 2) });
        Assert.Equal(4, buffer.WrittenCount);
        Assert.Equal(0, BinaryPrimitives.ReadUInt16LittleEndian(buffer.WrittenSpan));
        Assert.Equal(1, BinaryPrimitives.ReadUInt16LittleEndian(buffer.WrittenSpan[2..]));
    }

    [Fact]
    public void IColumnWriter_WriteValue_And_WriteColumn()
    {
        IColumnWriter w = new DateColumnWriter();

        var b1 = new ArrayBufferWriter<byte>();
        var p1 = new ProtocolWriter(b1);
        w.WriteValue(ref p1, new DateOnly(1970, 1, 6));
        Assert.Equal(5, BinaryPrimitives.ReadUInt16LittleEndian(b1.WrittenSpan));

        var b2 = new ArrayBufferWriter<byte>();
        var p2 = new ProtocolWriter(b2);
        w.WriteColumn(ref p2, new object?[] { new DateOnly(1970, 1, 3) });
        Assert.Equal(2, BinaryPrimitives.ReadUInt16LittleEndian(b2.WrittenSpan));
    }
}
