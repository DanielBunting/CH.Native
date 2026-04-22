using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Geo;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class GeometryColumnWriterTests
{
    [Fact]
    public void TypeName_IsGeometry() => Assert.Equal("Geometry", new GeometryColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsGeometry() => Assert.Equal(typeof(Geometry), new GeometryColumnWriter().ClrType);

    [Fact]
    public void Registry_ResolvesGeometryWriter()
    {
        var writer = ColumnWriterRegistry.Default.GetWriter("Geometry");
        Assert.IsType<GeometryColumnWriter>(writer);
    }

    [Fact]
    public void WriteColumn_AllNulls_WritesOnlyModeAndDiscriminators()
    {
        var rows = new[] { Geometry.Null, Geometry.Null };
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);

        new GeometryColumnWriter().WriteColumn(ref writer, rows);

        // Mode (8) + discriminators (2) = 10 bytes total. No arm data.
        Assert.Equal(10, buf.WrittenCount);
        Assert.Equal(0xFF, buf.WrittenSpan[8]);
        Assert.Equal(0xFF, buf.WrittenSpan[9]);
    }

    [Fact]
    public void WriteColumn_ObjectInterface_AcceptsGeometryAndNull()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);

        ((IColumnWriter)new GeometryColumnWriter()).WriteColumn(
            ref writer,
            new object?[] { Geometry.From(new Point(1, 2)), null });

        // First 10 bytes: mode + 2 discriminators.
        Assert.Equal((byte)GeometryKind.Point, buf.WrittenSpan[8]);
        Assert.Equal(0xFF, buf.WrittenSpan[9]);
    }

    [Fact]
    public void WriteColumn_ObjectInterface_RejectsWrongType()
    {
        var buf = new ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(buf);
        var iface = (IColumnWriter)new GeometryColumnWriter();

        Assert.Throws<ArgumentException>(() =>
        {
            var bufInner = new ArrayBufferWriter<byte>();
            var wInner = new ProtocolWriter(bufInner);
            iface.WriteColumn(ref wInner, new object?[] { "not a geometry" });
        });
    }
}
