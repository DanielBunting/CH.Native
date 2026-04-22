using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Geo;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

public class GeoColumnSkipperTests
{
    [Fact]
    public void Point_Skips_16BytesPerRow()
    {
        var points = new[] { new Point(1, 2), new Point(3, 4) };
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        new PointColumnWriter().WriteColumn(ref writer, points);
        writer.WriteUInt32(0xDEADBEEF);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buf.WrittenMemory));
        Assert.True(new PointColumnSkipper().TrySkipColumn(ref reader, 2));
        Assert.Equal(0xDEADBEEFu, reader.ReadUInt32());
    }

    [Fact]
    public void Ring_SkipsArrayOfPoints()
    {
        var rings = new[]
        {
            new[] { new Point(0, 0), new Point(1, 1) },
            Array.Empty<Point>(),
            new[] { new Point(5, 5) },
        };
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        new RingColumnWriter().WriteColumn(ref writer, rings);
        writer.WriteUInt32(0xDEADBEEF);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buf.WrittenMemory));
        Assert.True(new RingColumnSkipper().TrySkipColumn(ref reader, rings.Length));
        Assert.Equal(0xDEADBEEFu, reader.ReadUInt32());
    }

    [Fact]
    public void LineString_Skip_MatchesRingBytes()
    {
        var rings = new[] { new[] { new Point(0, 0), new Point(1, 1) } };
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        new LineStringColumnWriter().WriteColumn(ref writer, rings);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buf.WrittenMemory));
        Assert.True(new LineStringColumnSkipper().TrySkipColumn(ref reader, rings.Length));
    }

    [Fact]
    public void MultiLineString_SkipsNested()
    {
        var mls = new[]
        {
            new[]
            {
                new[] { new Point(0, 0), new Point(1, 1) },
                new[] { new Point(2, 2) },
            },
        };
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        new MultiLineStringColumnWriter().WriteColumn(ref writer, mls);
        writer.WriteUInt32(0xCAFEBABE);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buf.WrittenMemory));
        Assert.True(new MultiLineStringColumnSkipper().TrySkipColumn(ref reader, mls.Length));
        Assert.Equal(0xCAFEBABEu, reader.ReadUInt32());
    }

    [Fact]
    public void Polygon_SkipsNested()
    {
        var polys = new[]
        {
            new[]
            {
                new[] { new Point(0, 0), new Point(10, 0), new Point(10, 10), new Point(0, 10) },
                new[] { new Point(3, 3), new Point(4, 3), new Point(4, 4) },
            },
        };
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        new PolygonColumnWriter().WriteColumn(ref writer, polys);
        writer.WriteUInt32(0xCAFEBABE);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buf.WrittenMemory));
        Assert.True(new PolygonColumnSkipper().TrySkipColumn(ref reader, polys.Length));
        Assert.Equal(0xCAFEBABEu, reader.ReadUInt32());
    }

    [Fact]
    public void MultiPolygon_SkipsTripleNested()
    {
        var mp = new[]
        {
            new[]
            {
                new[]
                {
                    new[] { new Point(0, 0), new Point(1, 0), new Point(1, 1), new Point(0, 1) }
                },
                new[]
                {
                    new[] { new Point(10, 10), new Point(11, 10), new Point(11, 11), new Point(10, 11) }
                },
            },
        };
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        new MultiPolygonColumnWriter().WriteColumn(ref writer, mp);
        writer.WriteUInt32(0xCAFEBABE);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buf.WrittenMemory));
        Assert.True(new MultiPolygonColumnSkipper().TrySkipColumn(ref reader, mp.Length));
        Assert.Equal(0xCAFEBABEu, reader.ReadUInt32());
    }

    [Fact]
    public void Registry_ResolvesAllGeoSkippers()
    {
        Assert.IsType<PointColumnSkipper>(ColumnSkipperRegistry.Default.GetSkipper("Point"));
        Assert.IsType<RingColumnSkipper>(ColumnSkipperRegistry.Default.GetSkipper("Ring"));
        Assert.IsType<LineStringColumnSkipper>(ColumnSkipperRegistry.Default.GetSkipper("LineString"));
        Assert.IsType<MultiLineStringColumnSkipper>(ColumnSkipperRegistry.Default.GetSkipper("MultiLineString"));
        Assert.IsType<PolygonColumnSkipper>(ColumnSkipperRegistry.Default.GetSkipper("Polygon"));
        Assert.IsType<MultiPolygonColumnSkipper>(ColumnSkipperRegistry.Default.GetSkipper("MultiPolygon"));
    }
}
