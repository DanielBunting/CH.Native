using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Tuple(T1, T2, ...) values.
/// </summary>
/// <remarks>
/// Wire format: Each element is stored as a separate column (columnar layout).
/// All first elements, then all second elements, etc.
///
/// Returns object[] because the tuple arity and element types are not known at compile time.
/// Supports named tuples like Tuple(id UInt64, name String) where field names are preserved.
/// </remarks>
public sealed class TupleColumnReader : IColumnReader<object[]>
{
    private readonly IColumnReader[] _elementReaders;
    private readonly string[]? _fieldNames;

    /// <summary>
    /// Creates a Tuple reader with the specified element readers.
    /// </summary>
    /// <param name="elementReaders">Readers for each tuple element, in order.</param>
    /// <param name="fieldNames">Optional field names for named tuples.</param>
    public TupleColumnReader(IColumnReader[] elementReaders, string[]? fieldNames = null)
    {
        if (elementReaders == null || elementReaders.Length == 0)
            throw new ArgumentException("Tuple requires at least one element reader.", nameof(elementReaders));

        if (fieldNames != null && fieldNames.Length != elementReaders.Length)
            throw new ArgumentException("Field names count must match element readers count.", nameof(fieldNames));

        _elementReaders = elementReaders;
        _fieldNames = fieldNames;
    }

    /// <inheritdoc />
    public string TypeName
    {
        get
        {
            if (_fieldNames != null)
            {
                var fields = _elementReaders.Select((r, i) => $"{_fieldNames[i]} {r.TypeName}");
                return $"Tuple({string.Join(", ", fields)})";
            }
            return $"Tuple({string.Join(", ", _elementReaders.Select(r => r.TypeName))})";
        }
    }

    /// <inheritdoc />
    public Type ClrType => typeof(object[]);

    /// <summary>
    /// Gets the number of elements in the tuple.
    /// </summary>
    public int Arity => _elementReaders.Length;

    /// <summary>
    /// Gets the field names for named tuples. Returns null for positional tuples.
    /// </summary>
    public IReadOnlyList<string>? FieldNames => _fieldNames;

    /// <summary>
    /// Whether this tuple has named fields.
    /// </summary>
    public bool HasFieldNames => _fieldNames != null;

    /// <summary>
    /// Gets the index of a field by name. Returns -1 if not found or if this is a positional tuple.
    /// </summary>
    public int GetFieldIndex(string fieldName)
    {
        if (_fieldNames == null)
            return -1;

        for (int i = 0; i < _fieldNames.Length; i++)
        {
            if (string.Equals(_fieldNames[i], fieldName, StringComparison.Ordinal))
                return i;
        }
        return -1;
    }

    /// <inheritdoc />
    public object[] ReadValue(ref ProtocolReader reader)
    {
        var tuple = new object[_elementReaders.Length];
        for (int i = 0; i < _elementReaders.Length; i++)
        {
            // For single value, we read a 1-row column and get first element
            using var col = _elementReaders[i].ReadTypedColumn(ref reader, 1);
            tuple[i] = col.GetValue(0)!;
        }
        return tuple;
    }

    /// <inheritdoc />
    public TypedColumn<object[]> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return new TypedColumn<object[]>(Array.Empty<object[]>());

        // Read each element column fully (columnar layout)
        var elementColumns = new ITypedColumn[_elementReaders.Length];
        try
        {
            for (int e = 0; e < _elementReaders.Length; e++)
            {
                elementColumns[e] = _elementReaders[e].ReadTypedColumn(ref reader, rowCount);
            }

            // Transpose to row-major tuples
            var result = new object[rowCount][];
            for (int row = 0; row < rowCount; row++)
            {
                result[row] = new object[_elementReaders.Length];
                for (int e = 0; e < _elementReaders.Length; e++)
                {
                    result[row][e] = elementColumns[e].GetValue(row)!;
                }
            }

            return new TypedColumn<object[]>(result);
        }
        finally
        {
            // Dispose all element columns
            foreach (var col in elementColumns)
            {
                col?.Dispose();
            }
        }
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
