namespace CH.Native.Data.Geo;

/// <summary>
/// Discriminator kinds for a ClickHouse <c>Geometry</c> value. The numeric values match
/// the wire discriminators used by ClickHouse — alphabetical ordering of the arm type
/// names. A null arm in the wire stream is represented by <see cref="Null"/> (0xFF).
/// </summary>
public enum GeometryKind : byte
{
    /// <summary>LineString arm — CLR shape <c>Point[]</c>.</summary>
    LineString = 0,
    /// <summary>MultiLineString arm — CLR shape <c>Point[][]</c>.</summary>
    MultiLineString = 1,
    /// <summary>MultiPolygon arm — CLR shape <c>Point[][][]</c>.</summary>
    MultiPolygon = 2,
    /// <summary>Point arm — CLR shape <see cref="Geo.Point"/>.</summary>
    Point = 3,
    /// <summary>Polygon arm — CLR shape <c>Point[][]</c>.</summary>
    Polygon = 4,
    /// <summary>Ring arm — CLR shape <c>Point[]</c>.</summary>
    Ring = 5,
    /// <summary>NULL row marker. <see cref="Geometry.Value"/> is <c>null</c>.</summary>
    Null = 0xFF,
}

/// <summary>
/// Represents a ClickHouse <c>Geometry</c> discriminated-union value. Geometry is a
/// <c>Variant</c> over the six geo types (Point, Ring, LineString, Polygon,
/// MultiLineString, MultiPolygon). Use <see cref="Kind"/> to dispatch and the typed
/// <c>AsXxx</c> accessors (or pattern-match on <see cref="Value"/>) to extract the
/// underlying arm value.
/// </summary>
/// <param name="Kind">The arm this value belongs to, or <see cref="GeometryKind.Null"/>.</param>
/// <param name="Value">
/// The arm payload. Boxed <see cref="Geo.Point"/> for <see cref="GeometryKind.Point"/>;
/// <c>Point[]</c>/<c>Point[][]</c>/<c>Point[][][]</c> for the array-shaped arms; <c>null</c>
/// when <paramref name="Kind"/> is <see cref="GeometryKind.Null"/>.
/// </param>
public readonly record struct Geometry(GeometryKind Kind, object? Value)
{
    /// <summary>A NULL Geometry value.</summary>
    public static readonly Geometry Null = new(GeometryKind.Null, null);

    /// <summary>Wraps a <see cref="Geo.Point"/>.</summary>
    public static Geometry From(Point value) => new(GeometryKind.Point, value);

    /// <summary>Wraps a <c>Point[]</c> as a LineString.</summary>
    public static Geometry FromLineString(Point[] value) => new(GeometryKind.LineString, value);

    /// <summary>Wraps a <c>Point[]</c> as a Ring.</summary>
    public static Geometry FromRing(Point[] value) => new(GeometryKind.Ring, value);

    /// <summary>Wraps a <c>Point[][]</c> as a Polygon.</summary>
    public static Geometry FromPolygon(Point[][] value) => new(GeometryKind.Polygon, value);

    /// <summary>Wraps a <c>Point[][]</c> as a MultiLineString.</summary>
    public static Geometry FromMultiLineString(Point[][] value) => new(GeometryKind.MultiLineString, value);

    /// <summary>Wraps a <c>Point[][][]</c> as a MultiPolygon.</summary>
    public static Geometry FromMultiPolygon(Point[][][] value) => new(GeometryKind.MultiPolygon, value);

    /// <summary>True when this value is a NULL row.</summary>
    public bool IsNull => Kind == GeometryKind.Null;

    /// <summary>Extracts a Point; throws if <see cref="Kind"/> is not <see cref="GeometryKind.Point"/>.</summary>
    public Point AsPoint() => Kind == GeometryKind.Point
        ? (Point)Value!
        : throw new InvalidOperationException($"Geometry is {Kind}, not Point.");

    /// <summary>Extracts a LineString; throws if <see cref="Kind"/> is not <see cref="GeometryKind.LineString"/>.</summary>
    public Point[] AsLineString() => Kind == GeometryKind.LineString
        ? (Point[])Value!
        : throw new InvalidOperationException($"Geometry is {Kind}, not LineString.");

    /// <summary>Extracts a Ring; throws if <see cref="Kind"/> is not <see cref="GeometryKind.Ring"/>.</summary>
    public Point[] AsRing() => Kind == GeometryKind.Ring
        ? (Point[])Value!
        : throw new InvalidOperationException($"Geometry is {Kind}, not Ring.");

    /// <summary>Extracts a Polygon; throws if <see cref="Kind"/> is not <see cref="GeometryKind.Polygon"/>.</summary>
    public Point[][] AsPolygon() => Kind == GeometryKind.Polygon
        ? (Point[][])Value!
        : throw new InvalidOperationException($"Geometry is {Kind}, not Polygon.");

    /// <summary>Extracts a MultiLineString; throws if <see cref="Kind"/> is not <see cref="GeometryKind.MultiLineString"/>.</summary>
    public Point[][] AsMultiLineString() => Kind == GeometryKind.MultiLineString
        ? (Point[][])Value!
        : throw new InvalidOperationException($"Geometry is {Kind}, not MultiLineString.");

    /// <summary>Extracts a MultiPolygon; throws if <see cref="Kind"/> is not <see cref="GeometryKind.MultiPolygon"/>.</summary>
    public Point[][][] AsMultiPolygon() => Kind == GeometryKind.MultiPolygon
        ? (Point[][][])Value!
        : throw new InvalidOperationException($"Geometry is {Kind}, not MultiPolygon.");
}
