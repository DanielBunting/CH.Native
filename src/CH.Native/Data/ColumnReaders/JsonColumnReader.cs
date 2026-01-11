using System.Buffers;
using System.Text.Json;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for JSON values, returning <see cref="JsonDocument"/>.
/// </summary>
/// <remarks>
/// <para>
/// This reader supports ClickHouse 25.6+ JSON columns. When the server sends JSON with
/// serialization version 1 (string format), data is read as length-prefixed UTF-8 strings.
/// </para>
/// <para>
/// <b>Important:</b> <see cref="JsonDocument"/> is <see cref="IDisposable"/>.
/// Callers are responsible for disposing returned documents.
/// </para>
/// </remarks>
public sealed class JsonColumnReader : IColumnReader<JsonDocument>
{
    // JSON serialization versions from ClickHouse
    private const ulong JsonDeprecatedObjectSerializationVersion = 0;
    private const ulong JsonStringSerializationVersion = 1;
    private const ulong JsonObjectSerializationVersion = 3;

    /// <inheritdoc />
    public string TypeName => "JSON";

    /// <inheritdoc />
    public Type ClrType => typeof(JsonDocument);

    /// <inheritdoc />
    public JsonDocument ReadValue(ref ProtocolReader reader)
    {
        var json = reader.ReadString();
        return JsonDocument.Parse(json);
    }

    /// <inheritdoc />
    public TypedColumn<JsonDocument> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        // Read the serialization version (UInt64) that precedes the column data
        var serializationVersion = reader.ReadUInt64();

        if (serializationVersion == JsonStringSerializationVersion)
        {
            // Version 1: JSON is serialized as strings - simple and compatible
            return ReadStringSerializedColumn(ref reader, rowCount);
        }
        else if (serializationVersion == JsonDeprecatedObjectSerializationVersion ||
                 serializationVersion == JsonObjectSerializationVersion)
        {
            // Version 0 or 3: Complex object serialization
            throw new NotSupportedException(
                $"JSON serialization version {serializationVersion} (object format) is not supported. " +
                "Set 'output_format_native_write_json_as_string=1' in your ClickHouse settings or append " +
                "'SETTINGS output_format_native_write_json_as_string=1' to your query to use string serialization.");
        }
        else
        {
            throw new NotSupportedException(
                $"Unknown JSON serialization version: {serializationVersion}. " +
                "This may indicate an incompatible ClickHouse server version.");
        }
    }

    private static TypedColumn<JsonDocument> ReadStringSerializedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<JsonDocument>.Shared;
        var values = pool.Rent(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            var json = reader.ReadString();
            values[i] = JsonDocument.Parse(json);
        }

        return new TypedColumn<JsonDocument>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
