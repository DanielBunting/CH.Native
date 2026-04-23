using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Column skipper for JSON values.
/// </summary>
/// <remarks>
/// Supports:
/// <list type="bullet">
/// <item><description>Version 1 — string serialisation (length-prefixed UTF-8).</description></item>
/// <item><description>Version 0 — flat typed-path binary format (requires a factory for inner skippers).</description></item>
/// </list>
/// Version 3 (typed paths + Dynamic sub-column) is not yet fully implemented.
/// </remarks>
public sealed class JsonColumnSkipper : IColumnSkipper
{
    private const ulong JsonDeprecatedObjectSerializationVersion = 0;
    private const ulong JsonStringSerializationVersion = 1;
    private const ulong JsonObjectSerializationVersion = 3;

    private readonly ColumnSkipperFactory? _factory;

    public JsonColumnSkipper()
    {
    }

    public JsonColumnSkipper(ColumnSkipperFactory factory)
    {
        _factory = factory;
    }

    /// <inheritdoc />
    public string TypeName => "JSON";

    /// <inheritdoc />
    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        if (!reader.TryReadUInt64(out var serializationVersion))
            return false;

        if (serializationVersion == JsonStringSerializationVersion)
        {
            for (int i = 0; i < rowCount; i++)
            {
                if (!reader.TrySkipString())
                    return false;
            }
            return true;
        }

        if (serializationVersion == JsonDeprecatedObjectSerializationVersion && _factory is not null)
        {
            return TrySkipVersion0(ref reader, rowCount);
        }

        throw new NotSupportedException(
            $"Cannot skip JSON column with serialization version {serializationVersion}. " +
            "Version 1 (string) is supported directly, version 0 is supported when a factory is attached, " +
            "version 3 is not yet implemented. " +
            "Set 'output_format_native_write_json_as_string=1' in your ClickHouse settings to use string serialization.");
    }

    private bool TrySkipVersion0(ref ProtocolReader reader, int rowCount)
    {
        if (!reader.TryReadUInt64(out var pathCountU)) return false;
        var pathCount = checked((int)pathCountU);

        var typeNames = new string[pathCount];
        for (int i = 0; i < pathCount; i++)
        {
            if (!reader.TrySkipString()) return false; // path name
        }
        for (int i = 0; i < pathCount; i++)
        {
            try { typeNames[i] = reader.ReadString(); }
            catch { return false; }
        }

        for (int i = 0; i < pathCount; i++)
        {
            IColumnSkipper inner;
            try { inner = _factory!.CreateSkipper(typeNames[i]); }
            catch { return false; }

            if (!inner.TrySkipColumn(ref reader, rowCount)) return false;
        }

        return true;
    }
}
