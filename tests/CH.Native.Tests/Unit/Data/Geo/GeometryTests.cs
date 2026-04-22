using CH.Native.Data.Geo;
using Xunit;

namespace CH.Native.Tests.Unit.Data.Geo;

public class GeometryTests
{
    [Fact]
    public void GeometryKind_NumericValues_MatchAlphabeticalOrder()
    {
        // Discriminator ordering is load-bearing: it's used as an index into arm readers.
        Assert.Equal(0, (byte)GeometryKind.LineString);
        Assert.Equal(1, (byte)GeometryKind.MultiLineString);
        Assert.Equal(2, (byte)GeometryKind.MultiPolygon);
        Assert.Equal(3, (byte)GeometryKind.Point);
        Assert.Equal(4, (byte)GeometryKind.Polygon);
        Assert.Equal(5, (byte)GeometryKind.Ring);
        Assert.Equal(0xFF, (byte)GeometryKind.Null);
    }

    [Fact]
    public void Factories_WrapCorrectKind()
    {
        Assert.Equal(GeometryKind.Point, Geometry.From(new Point(1, 2)).Kind);
        Assert.Equal(GeometryKind.Ring, Geometry.FromRing(new Point[] { new(0, 0) }).Kind);
        Assert.Equal(GeometryKind.LineString, Geometry.FromLineString(new Point[] { new(0, 0) }).Kind);
        Assert.Equal(GeometryKind.Polygon, Geometry.FromPolygon(new Point[][] { new Point[] { new(0, 0) } }).Kind);
        Assert.Equal(GeometryKind.MultiLineString, Geometry.FromMultiLineString(new Point[][] { new Point[] { new(0, 0) } }).Kind);
        Assert.Equal(GeometryKind.MultiPolygon, Geometry.FromMultiPolygon(new Point[][][] { new Point[][] { new Point[] { new(0, 0) } } }).Kind);
    }

    [Fact]
    public void Null_HasNullKindAndNullValue()
    {
        var g = Geometry.Null;
        Assert.Equal(GeometryKind.Null, g.Kind);
        Assert.Null(g.Value);
        Assert.True(g.IsNull);
    }

    [Fact]
    public void Accessors_ReturnCorrectValue()
    {
        var p = new Point(3, 4);
        Assert.Equal(p, Geometry.From(p).AsPoint());

        var ring = new Point[] { new(0, 0), new(1, 1) };
        Assert.Equal(ring, Geometry.FromRing(ring).AsRing());

        var line = new Point[] { new(0, 0), new(1, 2) };
        Assert.Equal(line, Geometry.FromLineString(line).AsLineString());
    }

    [Fact]
    public void Accessors_ThrowOnWrongKind()
    {
        var g = Geometry.From(new Point(0, 0));
        Assert.Throws<InvalidOperationException>(() => g.AsRing());
        Assert.Throws<InvalidOperationException>(() => g.AsPolygon());
        Assert.Throws<InvalidOperationException>(() => g.AsMultiPolygon());
    }

    [Fact]
    public void Accessors_ThrowOnNull()
    {
        var g = Geometry.Null;
        Assert.Throws<InvalidOperationException>(() => g.AsPoint());
        Assert.Throws<InvalidOperationException>(() => g.AsRing());
    }

    [Fact]
    public void EqualityAndHashing_WorkForValueTypes()
    {
        var a = Geometry.From(new Point(1, 2));
        var b = Geometry.From(new Point(1, 2));
        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());

        var c = Geometry.From(new Point(3, 4));
        Assert.NotEqual(a, c);
    }

    [Fact]
    public void PatternMatch_WorksWithKindSwitch()
    {
        Geometry g = Geometry.FromPolygon(new Point[][] { new Point[] { new(0, 0), new(1, 0), new(1, 1), new(0, 1) } });
        var description = g.Kind switch
        {
            GeometryKind.Point => "point",
            GeometryKind.Ring => "ring",
            GeometryKind.Polygon => "polygon",
            _ => "other",
        };
        Assert.Equal("polygon", description);
    }
}
