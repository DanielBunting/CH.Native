using CH.Native.Data.Geo;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Polygon values (alias over Array(Ring), wire-identical to MultiLineString).
/// </summary>
internal sealed class PolygonColumnWriter : IColumnWriter<Point[][]>
{
    private readonly ArrayColumnWriter<Point[]> _inner =
        new(new ArrayColumnWriter<Point>(new PointColumnWriter()));

    /// <inheritdoc />
    public string TypeName => "Polygon";

    /// <inheritdoc />
    public Type ClrType => typeof(Point[][]);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, Point[][][] values)
        => _inner.WriteColumn(ref writer, values);

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, Point[][] value)
        => _inner.WriteValue(ref writer, value);

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
        => ((IColumnWriter)_inner).WriteColumn(ref writer, values);

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
        => ((IColumnWriter)_inner).WriteValue(ref writer, value);
}
