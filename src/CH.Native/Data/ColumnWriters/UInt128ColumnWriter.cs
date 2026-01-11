using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for UInt128 values.
/// </summary>
public sealed class UInt128ColumnWriter : IColumnWriter<UInt128>
{
    /// <inheritdoc />
    public string TypeName => "UInt128";

    /// <inheritdoc />
    public Type ClrType => typeof(UInt128);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, UInt128[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteUInt128(values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, UInt128 value)
    {
        writer.WriteUInt128(value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteUInt128((UInt128)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteUInt128((UInt128)value!);
    }
}
