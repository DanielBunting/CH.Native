using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Int32 (int) values.
/// </summary>
public sealed class Int32ColumnWriter : IColumnWriter<int>
{
    /// <inheritdoc />
    public string TypeName => "Int32";

    /// <inheritdoc />
    public Type ClrType => typeof(int);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, int[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteInt32(values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, int value)
    {
        writer.WriteInt32(value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteInt32((int)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteInt32((int)value!);
    }
}
