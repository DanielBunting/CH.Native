using CH.Native.Data.Geo;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Polygon values (alias over Array(Ring), wire-identical to MultiLineString).
/// Outer array is rings; first ring is the outer boundary, subsequent are holes.
/// </summary>
public sealed class PolygonColumnReader : IColumnReader<Point[][]>
{
    private readonly ArrayColumnReader<Point[]> _inner =
        new(new ArrayColumnReader<Point>(new PointColumnReader()));

    /// <inheritdoc />
    public string TypeName => "Polygon";

    /// <inheritdoc />
    public Type ClrType => typeof(Point[][]);

    /// <inheritdoc />
    public Point[][] ReadValue(ref ProtocolReader reader) => _inner.ReadValue(ref reader);

    /// <inheritdoc />
    public TypedColumn<Point[][]> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
        => _inner.ReadTypedColumn(ref reader, rowCount);

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
        => ReadTypedColumn(ref reader, rowCount);
}
