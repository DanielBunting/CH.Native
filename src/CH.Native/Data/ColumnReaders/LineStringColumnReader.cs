using CH.Native.Data.Geo;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for LineString values (alias over Array(Point), wire-identical to Ring).
/// </summary>
internal sealed class LineStringColumnReader : IColumnReader<Point[]>
{
    private readonly ArrayColumnReader<Point> _inner = new(new PointColumnReader());

    /// <inheritdoc />
    public string TypeName => "LineString";

    /// <inheritdoc />
    public Type ClrType => typeof(Point[]);

    /// <inheritdoc />
    public Point[] ReadValue(ref ProtocolReader reader) => _inner.ReadValue(ref reader);

    /// <inheritdoc />
    public TypedColumn<Point[]> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
        => _inner.ReadTypedColumn(ref reader, rowCount);

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
        => ReadTypedColumn(ref reader, rowCount);
}
