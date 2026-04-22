using System.Buffers;
using System.Text.Json;
using CH.Native.Data.Json;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for JSON values, returning <see cref="JsonDocument"/>.
/// </summary>
/// <remarks>
/// <para>
/// Supports three ClickHouse JSON serialization versions:
/// <list type="bullet">
/// <item><description><b>Version 1</b> — JSON-as-string (default when <c>output_format_native_write_json_as_string=1</c>).</description></item>
/// <item><description><b>Version 0</b> — legacy flat typed-path "object" format, routed through <see cref="JsonBinaryDecoder"/>.</description></item>
/// <item><description><b>Version 3</b> — typed paths plus a <c>Dynamic</c> sub-column. Experimental; requires Dynamic support.</description></item>
/// </list>
/// </para>
/// <para>
/// <b>Important:</b> <see cref="JsonDocument"/> is <see cref="IDisposable"/>. Callers are responsible
/// for disposing returned documents.
/// </para>
/// </remarks>
public sealed class JsonColumnReader : IColumnReader<JsonDocument>
{
    private const ulong JsonDeprecatedObjectSerializationVersion = 0;
    private const ulong JsonStringSerializationVersion = 1;
    private const ulong JsonObjectSerializationVersion = 3;

    private readonly ColumnReaderFactory? _factory;

    public JsonColumnReader()
    {
    }

    /// <summary>
    /// Creates a JSON reader with a factory reference; required to decode binary formats (v0/v3).
    /// </summary>
    public JsonColumnReader(ColumnReaderFactory factory)
    {
        _factory = factory;
    }

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

    // Version is the column-level state prefix; cached on the instance so the data
    // method can dispatch to the correct sub-format.
    private ulong _serializationVersion;

    /// <inheritdoc />
    public void ReadPrefix(ref ProtocolReader reader)
    {
        _serializationVersion = reader.ReadUInt64();
    }

    /// <inheritdoc />
    public TypedColumn<JsonDocument> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (_serializationVersion == JsonStringSerializationVersion)
            return ReadStringSerializedColumn(ref reader, rowCount);

        if (_serializationVersion == JsonDeprecatedObjectSerializationVersion)
            return ReadBinaryColumn(ref reader, rowCount, version: 0);

        if (_serializationVersion == JsonObjectSerializationVersion)
            return ReadBinaryColumn(ref reader, rowCount, version: 3);

        throw new NotSupportedException(
            $"Unknown JSON serialization version: {_serializationVersion}. " +
            "This may indicate an incompatible ClickHouse server version.");
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

    private TypedColumn<JsonDocument> ReadBinaryColumn(ref ProtocolReader reader, int rowCount, int version)
    {
        if (_factory is null)
            throw new NotSupportedException(
                $"JSON serialization version {version} (binary/object format) requires a ColumnReaderFactory. " +
                "This decoder path is routed by the connection layer; if you're constructing JsonColumnReader directly, " +
                "use the constructor that accepts a ColumnReaderFactory. Alternatively, set " +
                "'output_format_native_write_json_as_string=1' in your ClickHouse settings to fall back to string mode.");

        if (version == 0)
        {
            var docs = JsonBinaryDecoder.DecodeVersion0(ref reader, rowCount, _factory);
            return new TypedColumn<JsonDocument>(docs);
        }

        // Version 3 binary decoding is experimental: a typed-path section followed by a Dynamic
        // sub-column. Implementation deferred until wire-format bytes are verified against a
        // concrete ClickHouse reference; throw a descriptive error in the meantime so callers
        // know to fall back to string serialization.
        throw new NotSupportedException(
            "JSON serialization version 3 (typed paths + Dynamic sub-column) is not yet fully implemented. " +
            "The Variant and Dynamic readers are in place (see VariantColumnReader / DynamicColumnReader); " +
            "connecting them into the JSON path table requires a verified wire-format trace from " +
            "ClickHouse's SerializationJSON.cpp. " +
            "Set 'output_format_native_write_json_as_string=1' in your ClickHouse settings to use string serialization.");
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
