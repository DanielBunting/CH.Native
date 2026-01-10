using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Float32 (float) values.
/// </summary>
public sealed class Float32ColumnWriter : IColumnWriter<float>
{
    /// <inheritdoc />
    public string TypeName => "Float32";

    /// <inheritdoc />
    public Type ClrType => typeof(float);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, float[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteFloat32(values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, float value)
    {
        writer.WriteFloat32(value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteFloat32((float)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteFloat32((float)value!);
    }
}
