using System.Text.Json;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for JSON values.
/// </summary>
/// <remarks>
/// <para>
/// This writer serializes JSON data as length-prefixed UTF-8 strings,
/// compatible with ClickHouse 25.6+ using the
/// <c>output_format_native_use_flattened_dynamic_and_json_serialization=1</c> setting.
/// </para>
/// <para>
/// The non-generic <see cref="IColumnWriter.WriteValue"/> method accepts:
/// <list type="bullet">
/// <item><see cref="JsonDocument"/> - uses GetRawText()</item>
/// <item><see cref="JsonElement"/> - uses GetRawText()</item>
/// <item><see cref="string"/> - written directly as JSON string</item>
/// <item>Other objects - serialized using <see cref="JsonSerializer"/></item>
/// </list>
/// Null values are rejected — JSON columns are non-nullable on the wire.
/// </para>
/// </remarks>
internal sealed class JsonColumnWriter : IColumnWriter<JsonDocument>
{
    // JsonStringSerializationVersion (1) — tells the server the column data that
    // follows is per-row length-prefixed UTF-8 JSON strings, not the flattened
    // typed-path object format. Required column-level state prefix since CH 25.6.
    private const ulong JsonStringSerializationVersion = 1;

    /// <inheritdoc />
    public string TypeName => "JSON";

    /// <inheritdoc />
    public Type ClrType => typeof(JsonDocument);

    /// <inheritdoc />
    public void WritePrefix(ref ProtocolWriter writer) => writer.WriteUInt64(JsonStringSerializationVersion);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, JsonDocument[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is null)
                throw NullAt(i);
            writer.WriteString(values[i].RootElement.GetRawText());
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, JsonDocument value)
    {
        if (value is null)
            throw NullAt(rowIndex: -1);
        writer.WriteString(value.RootElement.GetRawText());
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is null)
                throw NullAt(i);
            writer.WriteString(SerializeNonNull(values[i]!));
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        if (value is null)
            throw NullAt(rowIndex: -1);
        writer.WriteString(SerializeNonNull(value));
    }

    private static string SerializeNonNull(object value) => value switch
    {
        JsonDocument doc => doc.RootElement.GetRawText(),
        JsonElement elem => elem.GetRawText(),
        string s => s,
        _ => JsonSerializer.Serialize(value),
    };

    private static InvalidOperationException NullAt(int rowIndex)
    {
        var where = rowIndex >= 0 ? $" at row {rowIndex}" : string.Empty;
        return new InvalidOperationException(
            $"JsonColumnWriter received null{where}. The JSON column type is non-nullable; " +
            $"ensure source values are non-null (use an empty JsonDocument like " +
            $"JsonDocument.Parse(\"{{}}\") if an empty object is the intended sentinel).");
    }
}
