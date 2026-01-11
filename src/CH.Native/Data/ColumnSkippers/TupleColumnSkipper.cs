using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Column skipper for Tuple(T1, T2, ...) values.
/// Wire format: Each element type's data concatenated.
/// </summary>
public sealed class TupleColumnSkipper : IColumnSkipper
{
    private readonly IColumnSkipper[] _elementSkippers;
    private readonly string _typeName;

    public TupleColumnSkipper(IColumnSkipper[] elementSkippers, string[] elementTypeNames)
    {
        _elementSkippers = elementSkippers;
        _typeName = $"Tuple({string.Join(", ", elementTypeNames)})";
    }

    public string TypeName => _typeName;

    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        // Each element type's data is stored contiguously
        foreach (var skipper in _elementSkippers)
        {
            if (!skipper.TrySkipColumn(ref reader, rowCount))
                return false;
        }
        return true;
    }
}

/// <summary>
/// Column skipper for Nested(name1 Type1, name2 Type2, ...) values.
/// Same wire format as Tuple - each element type's data concatenated.
/// </summary>
public sealed class NestedColumnSkipper : IColumnSkipper
{
    private readonly IColumnSkipper[] _elementSkippers;
    private readonly string _typeName;

    public NestedColumnSkipper(IColumnSkipper[] elementSkippers, string typeName)
    {
        _elementSkippers = elementSkippers;
        _typeName = typeName;
    }

    public string TypeName => _typeName;

    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        // Each element type's data is stored contiguously
        foreach (var skipper in _elementSkippers)
        {
            if (!skipper.TrySkipColumn(ref reader, rowCount))
                return false;
        }
        return true;
    }
}
