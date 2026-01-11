using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Bool values.
/// </summary>
public sealed class BoolColumnWriter : IColumnWriter<bool>
{
    /// <inheritdoc />
    public string TypeName => "Bool";

    /// <inheritdoc />
    public Type ClrType => typeof(bool);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, bool[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteByte(values[i] ? (byte)1 : (byte)0);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, bool value)
    {
        writer.WriteByte(value ? (byte)1 : (byte)0);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteByte((bool)values[i]! ? (byte)1 : (byte)0);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteByte((bool)value! ? (byte)1 : (byte)0);
    }
}
