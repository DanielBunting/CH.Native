using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Column skipper for Tuple(T1, T2, ...) values.
/// Wire format: Each element type's data concatenated.
/// </summary>
internal sealed class TupleColumnSkipper : IColumnSkipper
{
    private readonly IColumnSkipper[] _elementSkippers;
    private readonly string _typeName;

    public TupleColumnSkipper(IColumnSkipper[] elementSkippers, string[] elementTypeNames)
    {
        _elementSkippers = elementSkippers;
        _typeName = $"Tuple({string.Join(", ", elementTypeNames)})";
    }

    /// <inheritdoc />

    public string TypeName => _typeName;

    /// <inheritdoc />

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
internal sealed class NestedColumnSkipper : IColumnSkipper
{
    // Element skippers for each field's INNER type (e.g. String, Int32) — not Array(...)
    // skippers: the offsets are shared and read here, once.
    private readonly IColumnSkipper[] _fieldElementSkippers;
    private readonly string _typeName;

    public NestedColumnSkipper(IColumnSkipper[] fieldElementSkippers, string typeName)
    {
        _fieldElementSkippers = fieldElementSkippers;
        _typeName = typeName;
    }

    /// <inheritdoc />
    public string TypeName => _typeName;

    /// <inheritdoc />
    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return true;

        // Wire format: one shared offsets block (UInt64 per row), then each field's flat
        // values. Read the offsets once — only the last one (total element count) matters.
        if (rowCount > 1)
        {
            if (!reader.TrySkipBytes((long)(rowCount - 1) * 8))
                return false;
        }

        if (!reader.TryReadUInt64(out var totalElements))
            return false;

        // Each field has `totalElements` values, flattened, with no offsets of its own.
        if (totalElements > 0)
        {
            int total = ProtocolGuards.ToInt32(totalElements, "Nested total elements");
            foreach (var skipper in _fieldElementSkippers)
            {
                if (!skipper.TrySkipColumn(ref reader, total))
                    return false;
            }
        }

        return true;
    }
}
