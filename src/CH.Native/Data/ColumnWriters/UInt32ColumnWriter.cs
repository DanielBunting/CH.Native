using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for UInt32 (uint) values.
/// </summary>
public sealed class UInt32ColumnWriter : IColumnWriter<uint>
{
    /// <inheritdoc />
    public string TypeName => "UInt32";

    /// <inheritdoc />
    public Type ClrType => typeof(uint);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, uint[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteUInt32(values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, uint value)
    {
        writer.WriteUInt32(value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteUInt32((uint)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteUInt32((uint)value!);
    }
}
