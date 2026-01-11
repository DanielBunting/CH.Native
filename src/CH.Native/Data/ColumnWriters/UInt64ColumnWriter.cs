using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for UInt64 (ulong) values.
/// </summary>
public sealed class UInt64ColumnWriter : IColumnWriter<ulong>
{
    /// <inheritdoc />
    public string TypeName => "UInt64";

    /// <inheritdoc />
    public Type ClrType => typeof(ulong);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, ulong[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteUInt64(values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, ulong value)
    {
        writer.WriteUInt64(value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteUInt64((ulong)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteUInt64((ulong)value!);
    }
}
