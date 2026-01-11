using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Column skipper for JSON values.
/// </summary>
/// <remarks>
/// JSON columns have a serialization version prefix (UInt64) followed by data.
/// Version 1 (string serialization) is supported, where each value is a length-prefixed UTF-8 string.
/// </remarks>
public sealed class JsonColumnSkipper : IColumnSkipper
{
    // JSON serialization versions from ClickHouse
    private const ulong JsonStringSerializationVersion = 1;

    /// <inheritdoc />
    public string TypeName => "JSON";

    /// <inheritdoc />
    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        // Read the serialization version (UInt64) that precedes the column data
        if (!reader.TryReadUInt64(out var serializationVersion))
            return false;

        if (serializationVersion == JsonStringSerializationVersion)
        {
            // Version 1: JSON is serialized as strings
            for (int i = 0; i < rowCount; i++)
            {
                if (!reader.TrySkipString())
                    return false;
            }
            return true;
        }
        else
        {
            // Version 0 or 3 (object format) cannot be reliably skipped without
            // understanding the complex internal structure
            throw new NotSupportedException(
                $"Cannot skip JSON column with serialization version {serializationVersion}. " +
                "Only string serialization (version 1) is supported. " +
                "Set 'output_format_native_write_json_as_string=1' in your ClickHouse settings.");
        }
    }
}
