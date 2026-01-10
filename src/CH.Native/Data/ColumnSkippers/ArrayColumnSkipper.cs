using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Column skipper for Array(T) values.
/// Wire format: offsets array (UInt64 per row), then all element values.
/// </summary>
public sealed class ArrayColumnSkipper : IColumnSkipper
{
    private readonly IColumnSkipper _elementSkipper;
    private readonly string _typeName;

    public ArrayColumnSkipper(IColumnSkipper elementSkipper, string elementTypeName)
    {
        _elementSkipper = elementSkipper;
        _typeName = $"Array({elementTypeName})";
    }

    public string TypeName => _typeName;

    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return true;

        // Read offsets array - need to read these to know total element count
        // Each offset is UInt64 (8 bytes), and we need the last one
        var offsetsBytes = (long)rowCount * 8;

        // Skip all offsets except the last one
        if (rowCount > 1)
        {
            if (!reader.TrySkipBytes((long)(rowCount - 1) * 8))
                return false;
        }

        // Read the last offset to get total element count
        if (!reader.TryReadUInt64(out var totalElements))
            return false;

        // Skip element data
        if (totalElements > 0)
        {
            if (!_elementSkipper.TrySkipColumn(ref reader, (int)totalElements))
                return false;
        }

        return true;
    }
}
