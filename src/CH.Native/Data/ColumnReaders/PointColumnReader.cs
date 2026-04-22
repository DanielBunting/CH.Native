using System.Buffers;
using CH.Native.Data.Geo;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Point values (alias over Tuple(Float64, Float64)).
/// </summary>
/// <remarks>
/// Wire format matches the tuple: all X values, then all Y values (columnar layout).
/// </remarks>
public sealed class PointColumnReader : IColumnReader<Point>
{
    private readonly Float64ColumnReader _inner = new();

    /// <inheritdoc />
    public string TypeName => "Point";

    /// <inheritdoc />
    public Type ClrType => typeof(Point);

    /// <inheritdoc />
    public Point ReadValue(ref ProtocolReader reader)
    {
        var x = _inner.ReadValue(ref reader);
        var y = _inner.ReadValue(ref reader);
        return new Point(x, y);
    }

    /// <inheritdoc />
    public TypedColumn<Point> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return new TypedColumn<Point>(Array.Empty<Point>());

        using var xs = _inner.ReadTypedColumn(ref reader, rowCount);
        using var ys = _inner.ReadTypedColumn(ref reader, rowCount);

        var pool = ArrayPool<Point>.Shared;
        var values = pool.Rent(rowCount);
        var xSpan = xs.Values;
        var ySpan = ys.Values;
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = new Point(xSpan[i], ySpan[i]);
        }
        return new TypedColumn<Point>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
