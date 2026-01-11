using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Column skipper for Nullable(T) values.
/// Wire format: null bitmap (1 byte per row), then all values (including null slots).
/// </summary>
public sealed class NullableColumnSkipper : IColumnSkipper
{
    private readonly IColumnSkipper _innerSkipper;
    private readonly string _typeName;

    public NullableColumnSkipper(IColumnSkipper innerSkipper, string innerTypeName)
    {
        _innerSkipper = innerSkipper;
        _typeName = $"Nullable({innerTypeName})";
    }

    public string TypeName => _typeName;

    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        // Skip null bitmap (1 byte per row)
        if (!reader.TrySkipBytes(rowCount))
            return false;

        // Skip ALL values (including null slots - they still take space)
        return _innerSkipper.TrySkipColumn(ref reader, rowCount);
    }
}
