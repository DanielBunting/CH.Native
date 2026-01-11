using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Int8 (sbyte) values.
/// </summary>
public sealed class Int8ColumnWriter : IColumnWriter<sbyte>
{
    /// <inheritdoc />
    public string TypeName => "Int8";

    /// <inheritdoc />
    public Type ClrType => typeof(sbyte);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, sbyte[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteByte((byte)values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, sbyte value)
    {
        writer.WriteByte((byte)value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteByte((byte)(sbyte)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteByte((byte)(sbyte)value!);
    }
}
