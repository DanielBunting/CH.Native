using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Enum16 values.
/// Enum16 is stored as Int16 (short).
/// </summary>
public sealed class Enum16ColumnWriter : IColumnWriter<short>
{
    /// <inheritdoc />
    public string TypeName => "Enum16";

    /// <inheritdoc />
    public Type ClrType => typeof(short);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, short[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteInt16(values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, short value)
    {
        writer.WriteInt16(value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteInt16((short)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteInt16((short)value!);
    }
}
