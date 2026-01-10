using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Float64 (double) values.
/// </summary>
public sealed class Float64ColumnWriter : IColumnWriter<double>
{
    /// <inheritdoc />
    public string TypeName => "Float64";

    /// <inheritdoc />
    public Type ClrType => typeof(double);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, double[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteFloat64(values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, double value)
    {
        writer.WriteFloat64(value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteFloat64((double)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteFloat64((double)value!);
    }
}
