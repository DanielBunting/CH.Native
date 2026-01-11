using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Column skipper for Map(K, V) values.
/// Wire format: offsets array (UInt64 per row), then all keys, then all values.
/// </summary>
public sealed class MapColumnSkipper : IColumnSkipper
{
    private readonly IColumnSkipper _keySkipper;
    private readonly IColumnSkipper _valueSkipper;
    private readonly string _typeName;

    public MapColumnSkipper(IColumnSkipper keySkipper, IColumnSkipper valueSkipper, string keyTypeName, string valueTypeName)
    {
        _keySkipper = keySkipper;
        _valueSkipper = valueSkipper;
        _typeName = $"Map({keyTypeName}, {valueTypeName})";
    }

    public string TypeName => _typeName;

    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return true;

        // Skip all offsets except the last one
        if (rowCount > 1)
        {
            if (!reader.TrySkipBytes((long)(rowCount - 1) * 8))
                return false;
        }

        // Read the last offset to get total entry count
        if (!reader.TryReadUInt64(out var totalEntries))
            return false;

        if (totalEntries > 0)
        {
            // Skip keys
            if (!_keySkipper.TrySkipColumn(ref reader, (int)totalEntries))
                return false;

            // Skip values
            if (!_valueSkipper.TrySkipColumn(ref reader, (int)totalEntries))
                return false;
        }

        return true;
    }
}
