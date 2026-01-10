using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Column skipper for LowCardinality(T) values.
/// Wire format: version, flags/indexType, dictSize, dict values, indexCount, indices.
/// </summary>
public sealed class LowCardinalityColumnSkipper : IColumnSkipper
{
    private readonly IColumnSkipper _innerSkipper;
    private readonly string _typeName;

    // Index type constants from ClickHouse
    private const int IndexTypeUInt8 = 0;
    private const int IndexTypeUInt16 = 1;
    private const int IndexTypeUInt32 = 2;
    private const int IndexTypeUInt64 = 3;

    public LowCardinalityColumnSkipper(IColumnSkipper innerSkipper, string innerTypeName)
    {
        _innerSkipper = innerSkipper;
        _typeName = $"LowCardinality({innerTypeName})";
    }

    public string TypeName => _typeName;

    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return true;

        // Read version (UInt64)
        if (!reader.TryReadUInt64(out _))
            return false;

        // Read flags (UInt64) - contains index type in lower 8 bits
        if (!reader.TryReadUInt64(out var flags))
            return false;

        var indexType = (int)(flags & 0xFF);

        // Read dictionary size (UInt64)
        if (!reader.TryReadUInt64(out var dictSize))
            return false;

        // Skip dictionary values
        if (dictSize > 0)
        {
            if (!_innerSkipper.TrySkipColumn(ref reader, (int)dictSize))
                return false;
        }

        // Read index count (UInt64)
        if (!reader.TryReadUInt64(out var indexCount))
            return false;

        // Skip indices based on index type
        int indexByteSize = indexType switch
        {
            IndexTypeUInt8 => 1,
            IndexTypeUInt16 => 2,
            IndexTypeUInt32 => 4,
            IndexTypeUInt64 => 8,
            _ => throw new NotSupportedException($"Unknown LowCardinality index type: {indexType}")
        };

        return reader.TrySkipBytes((long)indexCount * indexByteSize);
    }
}
