using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for BFloat16 (brain float) values.
/// Truncates the low 16 mantissa bits of the float32 to match the ClickHouse server-side cast and clickhouse-cs.
/// </summary>
public sealed class BFloat16ColumnWriter : IColumnWriter<float>
{
    /// <inheritdoc />
    public string TypeName => "BFloat16";

    /// <inheritdoc />
    public Type ClrType => typeof(float);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, float[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, float value)
    {
        var bits = BitConverter.SingleToUInt32Bits(value);
        writer.WriteUInt16((ushort)(bits >> 16));
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, (float)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        WriteValue(ref writer, (float)value!);
    }
}
