using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Int128 values.
/// </summary>
public sealed class Int128ColumnWriter : IColumnWriter<Int128>
{
    /// <inheritdoc />
    public string TypeName => "Int128";

    /// <inheritdoc />
    public Type ClrType => typeof(Int128);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, Int128[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteInt128(values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, Int128 value)
    {
        writer.WriteInt128(value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteInt128((Int128)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteInt128((Int128)value!);
    }
}
