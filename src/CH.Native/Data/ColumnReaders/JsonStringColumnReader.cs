using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for JSON values, returning raw JSON strings.
/// </summary>
/// <remarks>
/// <para>
/// This reader requires ClickHouse 25.6+ with the
/// <c>output_format_native_use_flattened_dynamic_and_json_serialization=1</c> setting,
/// which serializes JSON columns as length-prefixed UTF-8 strings.
/// </para>
/// <para>
/// Use this reader instead of <see cref="JsonColumnReader"/> when you need raw JSON strings
/// without the parsing overhead of <see cref="System.Text.Json.JsonDocument"/>. This is useful for:
/// <list type="bullet">
/// <item>Pass-through scenarios (read JSON, write to another system)</item>
/// <item>Custom deserialization with Newtonsoft.Json or other libraries</item>
/// <item>Memory-sensitive applications where JsonDocument allocation is costly</item>
/// </list>
/// </para>
/// <para>
/// <b>Note:</b> This reader is not registered by default. To use it, create a custom
/// <see cref="ColumnReaderRegistry"/> with this reader registered for the "JSON" type.
/// </para>
/// </remarks>
public sealed class JsonStringColumnReader : IColumnReader<string>
{
    /// <summary>
    /// Thread-local pooled dictionary for string interning.
    /// Avoids allocating a new dictionary per column read.
    /// </summary>
    [ThreadStatic]
    private static Dictionary<string, string>? s_internPool;

    private static Dictionary<string, string> GetInternDictionary()
    {
        var dict = s_internPool;
        if (dict != null)
        {
            dict.Clear();
            return dict;
        }
        return s_internPool = new Dictionary<string, string>(1024, StringComparer.Ordinal);
    }

    /// <inheritdoc />
    public string TypeName => "JSON";

    /// <inheritdoc />
    public Type ClrType => typeof(string);

    /// <inheritdoc />
    public string ReadValue(ref ProtocolReader reader)
    {
        return reader.ReadString();
    }

    // JSON serialization versions from ClickHouse
    private const ulong JsonDeprecatedObjectSerializationVersion = 0;
    private const ulong JsonStringSerializationVersion = 1;
    private const ulong JsonObjectSerializationVersion = 3;

    /// <inheritdoc />
    public TypedColumn<string> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
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

    private TypedColumn<string> ReadStringSerializedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<string>.Shared;
        var values = pool.Rent(rowCount);

        // Use interning for larger columns to deduplicate repeated values
        if (rowCount >= 100)
        {
            var intern = GetInternDictionary();
            const int maxInternedStrings = 10000;

            for (int i = 0; i < rowCount; i++)
            {
                var s = reader.ReadString();
                if (intern.TryGetValue(s, out var existing))
                {
                    values[i] = existing;
                }
                else if (intern.Count < maxInternedStrings)
                {
                    intern[s] = s;
                    values[i] = s;
                }
                else
                {
                    values[i] = s;
                }
            }
        }
        else
        {
            for (int i = 0; i < rowCount; i++)
            {
                values[i] = reader.ReadString();
            }
        }

        return new TypedColumn<string>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
