using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for UInt8 (byte) values.
/// </summary>
public sealed class UInt8ColumnWriter : IColumnWriter<byte>
{
    /// <inheritdoc />
    public string TypeName => "UInt8";

    /// <inheritdoc />
    public Type ClrType => typeof(byte);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, byte[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteByte(values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, byte value)
    {
        writer.WriteByte(value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteByte((byte)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteByte((byte)value!);
    }
}
