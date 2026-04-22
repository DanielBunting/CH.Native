using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.Geo;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class MultiPolygonColumnReaderTests
{
    [Fact]
    public void TypeName_IsMultiPolygon() => Assert.Equal("MultiPolygon", new MultiPolygonColumnReader().TypeName);

    [Fact]
    public void ClrType_IsPointTripleJagged() => Assert.Equal(typeof(Point[][][]), new MultiPolygonColumnReader().ClrType);

    [Fact]
    public void ReadTypedColumn_TwoDisjointSquares_RoundTripsThroughWriter()
    {
        var square1 = new Point[][]
        {
            new Point[] { new(0, 0), new(1, 0), new(1, 1), new(0, 1) }
        };
        var square2 = new Point[][]
        {
            new Point[] { new(10, 10), new(11, 10), new(11, 11), new(10, 11) }
        };
        // Two rows: each row is a complete MultiPolygon.
        var rows = new Point[][][][]
        {
            new[] { square1, square2 },
            new[] { square1 },
        };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new MultiPolygonColumnWriter().WriteColumn(ref writer, rows);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new MultiPolygonColumnReader().ReadTypedColumn(ref reader, rows.Length);

        Assert.Equal(2, column.Count);
        Assert.Equal(2, column[0].Length);
        Assert.Equal(square1[0], column[0][0][0]);
        Assert.Equal(square2[0], column[0][1][0]);
        Assert.Single(column[1]);
        Assert.Equal(square1[0], column[1][0][0]);
    }

    [Fact]
    public void Registry_ResolvesMultiPolygon()
    {
        Assert.IsType<MultiPolygonColumnReader>(ColumnReaderRegistry.Default.GetReader("MultiPolygon"));
    }
}
