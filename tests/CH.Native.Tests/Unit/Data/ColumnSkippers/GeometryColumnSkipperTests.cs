using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Geo;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

public class GeometryColumnSkipperTests
{
    [Fact]
    public void TypeName_IsGeometry() => Assert.Equal("Geometry", new GeometryColumnSkipper().TypeName);

    [Fact]
    public void Registry_ResolvesGeometrySkipper()
    {
        var s = ColumnSkipperRegistry.Default.GetSkipper("Geometry");
        Assert.IsType<GeometryColumnSkipper>(s);
    }

    [Fact]
    public void TrySkipColumn_MixedArms_PositionsAtEnd()
    {
        var rows = new[]
        {
            Geometry.From(new Point(1, 2)),
            Geometry.FromRing(new Point[] { new(0, 0), new(1, 1) }),
            Geometry.Null,
            Geometry.FromPolygon(new Point[][] { new Point[] { new(0, 0), new(1, 1), new(2, 2) } }),
        };

        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        new GeometryColumnWriter().WriteColumn(ref writer, rows);
        writer.WriteUInt32(0xDEADBEEF);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buf.WrittenMemory));
        Assert.True(new GeometryColumnSkipper().TrySkipColumn(ref reader, rows.Length));
        Assert.Equal(0xDEADBEEFu, reader.ReadUInt32());
    }

    [Fact]
    public void TrySkipColumn_AllNulls_Succeeds()
    {
        var rows = new[] { Geometry.Null, Geometry.Null, Geometry.Null };

        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        new GeometryColumnWriter().WriteColumn(ref writer, rows);
        writer.WriteUInt32(0xCAFEBABE);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buf.WrittenMemory));
        Assert.True(new GeometryColumnSkipper().TrySkipColumn(ref reader, rows.Length));
        Assert.Equal(0xCAFEBABEu, reader.ReadUInt32());
    }

    [Fact]
    public void TrySkipColumn_EmptyBlock_Succeeds()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(Array.Empty<byte>()));
        Assert.True(new GeometryColumnSkipper().TrySkipColumn(ref reader, 0));
    }
}
