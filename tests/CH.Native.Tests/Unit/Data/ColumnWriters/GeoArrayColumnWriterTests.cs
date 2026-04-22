using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Geo;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class GeoArrayColumnWriterTests
{
    [Fact]
    public void Ring_RoundTrip()
    {
        var rings = new[]
        {
            new[] { new Point(0, 0), new Point(1, 0), new Point(1, 1), new Point(0, 1) },
            Array.Empty<Point>(),
            new[] { new Point(5, 5) },
        };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new RingColumnWriter().WriteColumn(ref writer, rings);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new RingColumnReader().ReadTypedColumn(ref reader, rings.Length);

        for (int i = 0; i < rings.Length; i++)
            Assert.Equal(rings[i], column[i]);
    }

    [Fact]
    public void LineString_RoundTrip_MatchesRing()
    {
        var lines = new[]
        {
            new[] { new Point(0, 0), new Point(1, 1) },
            new[] { new Point(2, 2), new Point(3, 3), new Point(4, 4) },
        };

        var ringBuf = new ArrayBufferWriter<byte>();
        var ringWriter = new ProtocolWriter(ringBuf);
        new RingColumnWriter().WriteColumn(ref ringWriter, lines);

        var lineBuf = new ArrayBufferWriter<byte>();
        var lineWriter = new ProtocolWriter(lineBuf);
        new LineStringColumnWriter().WriteColumn(ref lineWriter, lines);

        Assert.Equal(ringBuf.WrittenSpan.ToArray(), lineBuf.WrittenSpan.ToArray());
    }

    [Fact]
    public void MultiLineString_RoundTrip()
    {
        var mls = new[]
        {
            new[]
            {
                new[] { new Point(0, 0), new Point(1, 1) },
                new[] { new Point(2, 2), new Point(3, 3) },
            },
            new[]
            {
                new[] { new Point(10, 10), new Point(20, 20), new Point(30, 30) },
            },
        };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new MultiLineStringColumnWriter().WriteColumn(ref writer, mls);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new MultiLineStringColumnReader().ReadTypedColumn(ref reader, mls.Length);

        for (int i = 0; i < mls.Length; i++)
        {
            Assert.Equal(mls[i].Length, column[i].Length);
            for (int j = 0; j < mls[i].Length; j++)
                Assert.Equal(mls[i][j], column[i][j]);
        }
    }

    [Fact]
    public void Polygon_RoundTrip_WithHole()
    {
        var polys = new[]
        {
            new[]
            {
                new[] { new Point(0, 0), new Point(10, 0), new Point(10, 10), new Point(0, 10) },
                new[] { new Point(3, 3), new Point(4, 3), new Point(4, 4) },
            },
        };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new PolygonColumnWriter().WriteColumn(ref writer, polys);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new PolygonColumnReader().ReadTypedColumn(ref reader, polys.Length);

        Assert.Equal(2, column[0].Length);
        Assert.Equal(polys[0][0], column[0][0]);
        Assert.Equal(polys[0][1], column[0][1]);
    }

    [Fact]
    public void Registry_ResolvesAllGeoWriters()
    {
        Assert.IsType<RingColumnWriter>(ColumnWriterRegistry.Default.GetWriter("Ring"));
        Assert.IsType<LineStringColumnWriter>(ColumnWriterRegistry.Default.GetWriter("LineString"));
        Assert.IsType<PolygonColumnWriter>(ColumnWriterRegistry.Default.GetWriter("Polygon"));
        Assert.IsType<MultiLineStringColumnWriter>(ColumnWriterRegistry.Default.GetWriter("MultiLineString"));
        Assert.IsType<MultiPolygonColumnWriter>(ColumnWriterRegistry.Default.GetWriter("MultiPolygon"));
    }
}
