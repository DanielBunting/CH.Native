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
/// <item><c>null</c> - written as "{}"</item>
/// <item>Other objects - serialized using <see cref="JsonSerializer"/></item>
/// </list>
/// </para>
/// </remarks>
public sealed class JsonColumnWriter : IColumnWriter<JsonDocument>
{
    /// <inheritdoc />
    public string TypeName => "JSON";

    /// <inheritdoc />
    public Type ClrType => typeof(JsonDocument);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, JsonDocument[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, JsonDocument value)
    {
        var json = value?.RootElement.GetRawText() ?? "{}";
        writer.WriteString(json);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            ((IColumnWriter)this).WriteValue(ref writer, values[i]);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        var json = value switch
        {
            JsonDocument doc => doc.RootElement.GetRawText(),
            JsonElement elem => elem.GetRawText(),
            string s => s,
            null => "{}",
            _ => JsonSerializer.Serialize(value)
        };
        writer.WriteString(json);
    }
}
