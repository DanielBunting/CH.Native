using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.Geo;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class PolygonMultiLineStringColumnReaderTests
{
    [Fact]
    public void Polygon_TypeName_IsPolygon() => Assert.Equal("Polygon", new PolygonColumnReader().TypeName);

    [Fact]
    public void MultiLineString_TypeName_IsMultiLineString()
        => Assert.Equal("MultiLineString", new MultiLineStringColumnReader().TypeName);

    [Fact]
    public void Both_ClrType_IsPointJaggedArray()
    {
        Assert.Equal(typeof(Point[][]), new PolygonColumnReader().ClrType);
        Assert.Equal(typeof(Point[][]), new MultiLineStringColumnReader().ClrType);
    }

    [Fact]
    public void Polygon_ReadTypedColumn_UnitSquareWithHole()
    {
        var outerRing = new Point[] { new(0, 0), new(10, 0), new(10, 10), new(0, 10) };
        var innerRing = new Point[] { new(3, 3), new(4, 3), new(4, 4) };
        var bytes = BuildArrayOfRings(new[] { outerRing, innerRing });
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = new PolygonColumnReader().ReadTypedColumn(ref reader, 1);

        Assert.Equal(1, column.Count);
        Assert.Equal(2, column[0].Length);
        Assert.Equal(outerRing, column[0][0]);
        Assert.Equal(innerRing, column[0][1]);
    }

    [Fact]
    public void MultiLineString_MatchesPolygonBytes()
    {
        var lines = new[]
        {
            new Point[] { new(0, 0), new(1, 1) },
            new Point[] { new(2, 2), new(3, 3), new(4, 4) }
        };
        var bytes = BuildArrayOfRings(lines);

        var r1 = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var mls = new MultiLineStringColumnReader().ReadTypedColumn(ref r1, 1);

        var r2 = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var poly = new PolygonColumnReader().ReadTypedColumn(ref r2, 1);

        Assert.Equal(mls[0][0], poly[0][0]);
        Assert.Equal(mls[0][1], poly[0][1]);
    }

    [Fact]
    public void Registry_ResolvesBoth()
    {
        Assert.IsType<PolygonColumnReader>(ColumnReaderRegistry.Default.GetReader("Polygon"));
        Assert.IsType<MultiLineStringColumnReader>(ColumnReaderRegistry.Default.GetReader("MultiLineString"));
    }

    // Build wire bytes for Array(Array(Point)) = one row with N rings.
    // Outer offsets: [N] (UInt64), then inner Array(Point) block.
    internal static byte[] BuildArrayOfRings(Point[][] rings)
    {
        var innerBytes = RingLineStringColumnReaderTests.BuildArrayOfPoints(rings);
        var buffer = new byte[8 + innerBytes.Length];
        BitConverter.TryWriteBytes(buffer.AsSpan(0, 8), (ulong)rings.Length);
        innerBytes.CopyTo(buffer.AsSpan(8));
        return buffer;
    }
}
