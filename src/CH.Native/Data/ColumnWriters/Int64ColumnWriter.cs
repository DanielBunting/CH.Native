using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Int64 (long) values.
/// </summary>
public sealed class Int64ColumnWriter : IColumnWriter<long>
{
    /// <inheritdoc />
    public string TypeName => "Int64";

    /// <inheritdoc />
    public Type ClrType => typeof(long);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, long[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteInt64(values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, long value)
    {
        writer.WriteInt64(value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteInt64((long)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteInt64((long)value!);
    }
}
