using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for String values.
/// </summary>
public sealed class StringColumnWriter : IColumnWriter<string>
{
    /// <inheritdoc />
    public string TypeName => "String";

    /// <inheritdoc />
    public Type ClrType => typeof(string);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, string[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteString(values[i] ?? string.Empty);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, string value)
    {
        writer.WriteString(value ?? string.Empty);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteString((string?)values[i] ?? string.Empty);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteString((string?)value ?? string.Empty);
    }
}
