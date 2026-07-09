using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class TimeColumnWriterCoverageTests
{
    [Fact]
    public void WriteColumn_WritesEachValue()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        new TimeColumnWriter().WriteColumn(ref pw, new[] { new TimeOnly(0, 0, 1), new TimeOnly(0, 1, 0) });
        Assert.Equal(8, buffer.WrittenCount);
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(buffer.WrittenSpan));
        Assert.Equal(60, BinaryPrimitives.ReadInt32LittleEndian(buffer.WrittenSpan[4..]));
    }

    [Fact]
    public void IColumnWriter_WriteValue_And_WriteColumn()
    {
        IColumnWriter w = new TimeColumnWriter();

        var b1 = new ArrayBufferWriter<byte>();
        var p1 = new ProtocolWriter(b1);
        w.WriteValue(ref p1, new TimeOnly(0, 0, 5));
        Assert.Equal(5, BinaryPrimitives.ReadInt32LittleEndian(b1.WrittenSpan));

        var b2 = new ArrayBufferWriter<byte>();
        var p2 = new ProtocolWriter(b2);
        w.WriteColumn(ref p2, new object?[] { new TimeOnly(0, 0, 2) });
        Assert.Equal(2, BinaryPrimitives.ReadInt32LittleEndian(b2.WrittenSpan));
    }
}
