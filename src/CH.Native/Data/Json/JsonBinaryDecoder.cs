using System.Buffers;
using System.Text.Json;
using CH.Native.Protocol;

namespace CH.Native.Data.Json;

/// <summary>
/// Decoder for the binary (object) JSON serialization formats — versions 0 and 3.
/// </summary>
/// <remarks>
/// <para>
/// Version 1 (plain-string) is handled directly by <see cref="CH.Native.Data.ColumnReaders.JsonColumnReader"/>.
/// Version 0 is the flat typed-path format; version 3 adds a dynamic sub-column driven by
/// <see cref="CH.Native.Data.ColumnReaders.DynamicColumnReader"/>.
/// </para>
/// <para>
/// <b>Experimental:</b> the binary formats are under active evolution in ClickHouse. The
/// interpretation used here follows the spec in <c>.tmp/06-json-enhancements.md</c>. Wire
/// format details should be confirmed against <c>SerializationObject.cpp</c> and
/// <c>SerializationJSON.cpp</c> before relying on production round-trips.
/// </para>
/// </remarks>
public static class JsonBinaryDecoder
{
    /// <summary>
    /// Decodes a version-0 object-serialised JSON column into a dense array of
    /// <see cref="JsonDocument"/> (one per row).
    /// </summary>
    /// <param name="reader">The protocol reader positioned just after the already-consumed UInt64 version.</param>
    /// <param name="rowCount">Number of rows in this block.</param>
    /// <param name="factory">Column-reader factory used to resolve typed-path readers.</param>
    public static JsonDocument[] DecodeVersion0(ref ProtocolReader reader, int rowCount, ColumnReaderFactory factory)
    {
        var pathCount = checked((int)reader.ReadUInt64());

        var pathNames = new string[pathCount];
        for (int i = 0; i < pathCount; i++)
            pathNames[i] = reader.ReadString();

        var typeNames = new string[pathCount];
        for (int i = 0; i < pathCount; i++)
            typeNames[i] = reader.ReadString();

        var columns = new ITypedColumn?[pathCount];
        try
        {
            for (int i = 0; i < pathCount; i++)
            {
                var pathReader = factory.CreateReader(typeNames[i]);
                columns[i] = pathReader.ReadTypedColumn(ref reader, rowCount);
            }

            var docs = new JsonDocument[rowCount];

            // Reuse a single pooled scratch buffer across all rows. PooledBufferWriter
            // implements IBufferWriter<byte>, so Utf8JsonWriter writes into it directly
            // without any intermediate MemoryStream allocation (the baseline version
            // allocated a new MemoryStream + Utf8JsonWriter per row).
            //
            // JsonDocument.Parse(ReadOnlyMemory<byte>) does NOT copy its input — it holds a
            // live reference. So each row must have its own byte array; we build it from the
            // reused scratch buffer, which gives us one exact-sized byte[] per row instead
            // of the MemoryStream + growth-buffer churn.
            using var scratch = new PooledBufferWriter(4096);
            using var jsonWriter = new Utf8JsonWriter(scratch);

            for (int r = 0; r < rowCount; r++)
            {
                scratch.Reset();
                jsonWriter.Reset(scratch);

                jsonWriter.WriteStartObject();
                for (int p = 0; p < pathCount; p++)
                {
                    var value = columns[p]!.GetValue(r);
                    if (value is null) continue;
                    WritePathValue(jsonWriter, pathNames[p], value);
                }
                jsonWriter.WriteEndObject();
                jsonWriter.Flush();

                var bytes = scratch.WrittenSpan.ToArray();
                docs[r] = JsonDocument.Parse(bytes);
            }

            return docs;
        }
        finally
        {
            for (int i = 0; i < pathCount; i++)
                columns[i]?.Dispose();
        }
    }

    /// <summary>
    /// Writes a typed-path value as a nested JSON property, expanding the dotted path
    /// (e.g. <c>a.b.c</c>) into the corresponding nested objects.
    /// </summary>
    private static void WritePathValue(Utf8JsonWriter writer, string path, object value)
    {
        var segments = path.Split('.');
        for (int i = 0; i < segments.Length - 1; i++)
        {
            writer.WriteStartObject(segments[i]);
        }

        writer.WritePropertyName(segments[^1]);
        WriteValue(writer, value);

        for (int i = 0; i < segments.Length - 1; i++)
            writer.WriteEndObject();
    }

    private static void WriteValue(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
            case string s:
                writer.WriteStringValue(s);
                break;
            case bool b:
                writer.WriteBooleanValue(b);
                break;
            case byte by:
                writer.WriteNumberValue(by);
                break;
            case sbyte sb:
                writer.WriteNumberValue(sb);
                break;
            case short sh:
                writer.WriteNumberValue(sh);
                break;
            case ushort ush:
                writer.WriteNumberValue(ush);
                break;
            case int i32:
                writer.WriteNumberValue(i32);
                break;
            case uint u32:
                writer.WriteNumberValue(u32);
                break;
            case long i64:
                writer.WriteNumberValue(i64);
                break;
            case ulong u64:
                writer.WriteNumberValue(u64);
                break;
            case float f32:
                writer.WriteNumberValue(f32);
                break;
            case double f64:
                writer.WriteNumberValue(f64);
                break;
            case decimal dec:
                writer.WriteNumberValue(dec);
                break;
            case DateTime dt:
                writer.WriteStringValue(dt);
                break;
            case DateOnly d:
                writer.WriteStringValue(d.ToString("yyyy-MM-dd"));
                break;
            case Guid g:
                writer.WriteStringValue(g);
                break;
            case byte[] bytes:
                writer.WriteStringValue(Convert.ToBase64String(bytes));
                break;
            case System.Net.IPAddress ip:
                writer.WriteStringValue(ip.ToString());
                break;
            case System.Numerics.BigInteger big:
                writer.WriteRawValue(big.ToString());
                break;
            case Array arr:
                writer.WriteStartArray();
                foreach (var item in arr)
                    WriteValue(writer, item);
                writer.WriteEndArray();
                break;
            case System.Collections.IDictionary dict:
                writer.WriteStartObject();
                foreach (System.Collections.DictionaryEntry entry in dict)
                {
                    writer.WritePropertyName(entry.Key?.ToString() ?? "");
                    WriteValue(writer, entry.Value);
                }
                writer.WriteEndObject();
                break;
            case JsonElement je:
                je.WriteTo(writer);
                break;
            default:
                // Fallback — stringify via JsonSerializer for unknown shapes.
                var json = JsonSerializer.Serialize(value);
                using (var doc = JsonDocument.Parse(json))
                    doc.RootElement.WriteTo(writer);
                break;
        }
    }
}
