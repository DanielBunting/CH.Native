using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Column skipper for Point (Tuple(Float64, Float64)) — 16 bytes per row.
/// </summary>
internal sealed class PointColumnSkipper : FixedSizeColumnSkipper
{
    public PointColumnSkipper() : base(16) { }
    public override string TypeName => "Point";
}

/// <summary>
/// Column skipper for Ring (Array(Point)).
/// </summary>
internal sealed class RingColumnSkipper : IColumnSkipper
{
    private readonly ArrayColumnSkipper _inner = new(new PointColumnSkipper(), "Point");
    public string TypeName => "Ring";
    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
        => _inner.TrySkipColumn(ref reader, rowCount);
}

/// <summary>
/// Column skipper for LineString (Array(Point), wire-identical to Ring).
/// </summary>
internal sealed class LineStringColumnSkipper : IColumnSkipper
{
    private readonly ArrayColumnSkipper _inner = new(new PointColumnSkipper(), "Point");
    public string TypeName => "LineString";
    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
        => _inner.TrySkipColumn(ref reader, rowCount);
}

/// <summary>
/// Column skipper for MultiLineString (Array(Array(Point))).
/// </summary>
internal sealed class MultiLineStringColumnSkipper : IColumnSkipper
{
    private readonly ArrayColumnSkipper _inner =
        new(new ArrayColumnSkipper(new PointColumnSkipper(), "Point"), "Array(Point)");
    public string TypeName => "MultiLineString";
    /// <inheritdoc />
    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
        => _inner.TrySkipColumn(ref reader, rowCount);
}

/// <summary>
/// Column skipper for Polygon (Array(Ring), wire-identical to MultiLineString).
/// </summary>
internal sealed class PolygonColumnSkipper : IColumnSkipper
{
    private readonly ArrayColumnSkipper _inner =
        new(new ArrayColumnSkipper(new PointColumnSkipper(), "Point"), "Ring");
    public string TypeName => "Polygon";
    /// <inheritdoc />
    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
        => _inner.TrySkipColumn(ref reader, rowCount);
}

/// <summary>
/// Column skipper for MultiPolygon (Array(Polygon)).
/// </summary>
internal sealed class MultiPolygonColumnSkipper : IColumnSkipper
{
    private readonly ArrayColumnSkipper _inner =
        new(new ArrayColumnSkipper(
                new ArrayColumnSkipper(new PointColumnSkipper(), "Point"),
                "Ring"),
            "Polygon");
    /// <inheritdoc />
    public string TypeName => "MultiPolygon";
    /// <inheritdoc />
    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
        => _inner.TrySkipColumn(ref reader, rowCount);
}
