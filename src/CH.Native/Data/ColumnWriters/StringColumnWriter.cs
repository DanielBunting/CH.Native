using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for String values.
/// </summary>
/// <remarks>
/// This writer is strict: a null value into any of its write paths throws
/// <see cref="InvalidOperationException"/>. For Nullable(String) columns the
/// caller must wrap with <see cref="NullableRefColumnWriter{T}"/>, which writes
/// the bitmap and substitutes <see cref="NullPlaceholder"/> (the empty string)
/// for null slots before delegating here.
/// </remarks>
internal sealed class StringColumnWriter : IColumnWriter<string>
{
    /// <inheritdoc />
    public string TypeName => "String";

    /// <inheritdoc />
    public Type ClrType => typeof(string);

    /// <inheritdoc />
    public string NullPlaceholder => string.Empty;

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, string[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is null)
                throw NullAt(i);
            writer.WriteString(values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, string value)
    {
        if (value is null)
            throw NullAt(rowIndex: -1);
        writer.WriteString(value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is null)
                throw NullAt(i);
            writer.WriteString((string)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        if (value is null)
            throw NullAt(rowIndex: -1);
        writer.WriteString((string)value);
    }

    private static InvalidOperationException NullAt(int rowIndex)
    {
        var where = rowIndex >= 0 ? $" at row {rowIndex}" : string.Empty;
        return new InvalidOperationException(
            $"StringColumnWriter received null{where}. The String column type is " +
            $"non-nullable; declare the column as Nullable(String) and wrap this " +
            $"writer with NullableRefColumnWriter, or ensure source values are non-null.");
    }
}
