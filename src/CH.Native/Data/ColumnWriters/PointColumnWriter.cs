using CH.Native.Data.Geo;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Point values (alias over Tuple(Float64, Float64)).
/// </summary>
/// <remarks>
/// Wire format matches the tuple: all X values, then all Y values (columnar layout).
/// Also accepts Tuple&lt;double,double&gt; and ValueTuple&lt;double,double&gt; for interop
/// with other drivers' surface.
/// </remarks>
internal sealed class PointColumnWriter : IColumnWriter<Point>
{
    /// <inheritdoc />
    public string TypeName => "Point";

    /// <inheritdoc />
    public Type ClrType => typeof(Point);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, Point[] values)
    {
        for (int i = 0; i < values.Length; i++)
            writer.WriteFloat64(values[i].X);
        for (int i = 0; i < values.Length; i++)
            writer.WriteFloat64(values[i].Y);
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, Point value)
    {
        writer.WriteFloat64(value.X);
        writer.WriteFloat64(value.Y);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            var p = Coerce(values[i], rowIndex: i);
            writer.WriteFloat64(p.X);
        }
        for (int i = 0; i < values.Length; i++)
        {
            var p = Coerce(values[i], rowIndex: i);
            writer.WriteFloat64(p.Y);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        var p = Coerce(value, rowIndex: -1);
        writer.WriteFloat64(p.X);
        writer.WriteFloat64(p.Y);
    }

    private static Point Coerce(object? value, int rowIndex) => value switch
    {
        null => throw NullAt(rowIndex),
        Point p => p,
        (double x, double y) => new Point(x, y),
        Tuple<double, double> t => new Point(t.Item1, t.Item2),
        _ => throw new ArgumentException(
            $"Cannot convert {value.GetType()} to Point. Expected Point, (double,double), or Tuple<double,double>.",
            nameof(value)),
    };

    private static InvalidOperationException NullAt(int rowIndex)
    {
        var where = rowIndex >= 0 ? $" at row {rowIndex}" : string.Empty;
        return new InvalidOperationException(
            $"PointColumnWriter received null{where}. The Point column type is non-nullable; " +
            $"declare the column as Nullable(Point) and wrap this writer with NullableRefColumnWriter, " +
            $"or ensure source values are non-null.");
    }
}
