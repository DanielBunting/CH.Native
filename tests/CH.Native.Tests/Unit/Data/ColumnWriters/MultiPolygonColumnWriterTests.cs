using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Geo;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class MultiPolygonColumnWriterTests
{
    [Fact]
    public void TypeName_IsMultiPolygon() =>
        Assert.Equal("MultiPolygon", new MultiPolygonColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsThreeDeepPointArray() =>
        Assert.Equal(typeof(Point[][][]), new MultiPolygonColumnWriter().ClrType);

    [Fact]
    public void RoundTrip_TwoPolygonsOneWithHole()
    {
        // MultiPolygon = Array(Polygon) = Array(Array(Ring)) = Array(Array(Array(Point)))
        var multi = new[]
        {
            // First polygon: outer ring + inner hole
            new[]
            {
                new[] { new Point(0, 0), new Point(10, 0), new Point(10, 10), new Point(0, 10) },
                new[] { new Point(3, 3), new Point(4, 3), new Point(4, 4) },
            },
            // Second polygon: just outer ring
            new[]
            {
                new[] { new Point(20, 20), new Point(30, 20), new Point(30, 30) },
            },
        };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new MultiPolygonColumnWriter().WriteColumn(ref writer, new[] { multi });

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new MultiPolygonColumnReader().ReadTypedColumn(ref reader, 1);

        Assert.Equal(multi.Length, column[0].Length);
        for (int p = 0; p < multi.Length; p++)
        {
            Assert.Equal(multi[p].Length, column[0][p].Length);
            for (int r = 0; r < multi[p].Length; r++)
                Assert.Equal(multi[p][r], column[0][p][r]);
        }
    }

    [Fact]
    public void RoundTrip_EmptyMultiPolygon()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new MultiPolygonColumnWriter().WriteColumn(ref writer, new[] { Array.Empty<Point[][]>() });

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new MultiPolygonColumnReader().ReadTypedColumn(ref reader, 1);

        Assert.Empty(column[0]);
    }
}
