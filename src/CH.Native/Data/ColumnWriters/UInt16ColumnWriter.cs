using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for UInt16 (ushort) values.
/// </summary>
public sealed class UInt16ColumnWriter : IColumnWriter<ushort>
{
    /// <inheritdoc />
    public string TypeName => "UInt16";

    /// <inheritdoc />
    public Type ClrType => typeof(ushort);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, ushort[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteUInt16(values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, ushort value)
    {
        writer.WriteUInt16(value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteUInt16((ushort)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteUInt16((ushort)value!);
    }
}
