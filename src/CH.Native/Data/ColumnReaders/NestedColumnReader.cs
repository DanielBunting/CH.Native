using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Nested(field1 Type1, field2 Type2, ...) values.
/// </summary>
/// <remarks>
/// Wire format: Nested types are stored as tuples of arrays in columnar layout.
/// Each field becomes an array, and the arrays are stored sequentially.
///
/// For example, Nested(id UInt64, name String) is stored as:
/// - Array offsets for id (UInt64[])
/// - Array values for id
/// - Array offsets for name (String[])
/// - Array values for name
///
/// Returns object[][] where each row is an array of (fieldValue1[], fieldValue2[], ...).
/// Each fieldValue is an array because Nested represents repeated structured data.
/// </remarks>
public sealed class NestedColumnReader : IColumnReader<object[]>
{
    private readonly IColumnReader[] _fieldReaders;
    private readonly string[] _fieldNames;

    /// <summary>
    /// Creates a Nested reader with the specified field readers and names.
    /// </summary>
    /// <param name="fieldReaders">Array readers for each nested field, in order.</param>
    /// <param name="fieldNames">Names for each field.</param>
    public NestedColumnReader(IColumnReader[] fieldReaders, string[] fieldNames)
    {
        if (fieldReaders == null || fieldReaders.Length == 0)
            throw new ArgumentException("Nested requires at least one field reader.", nameof(fieldReaders));

        if (fieldNames == null || fieldNames.Length != fieldReaders.Length)
            throw new ArgumentException("Field names count must match field readers count.", nameof(fieldNames));

        _fieldReaders = fieldReaders;
        _fieldNames = fieldNames;
    }

    /// <inheritdoc />
    public string TypeName
    {
        get
        {
            var fields = _fieldReaders.Select((r, i) =>
            {
                // Extract inner type from Array reader
                var innerTypeName = r.TypeName;
                if (innerTypeName.StartsWith("Array(") && innerTypeName.EndsWith(")"))
                {
                    innerTypeName = innerTypeName.Substring(6, innerTypeName.Length - 7);
                }
                return $"{_fieldNames[i]} {innerTypeName}";
            });
            return $"Nested({string.Join(", ", fields)})";
        }
    }

    /// <inheritdoc />
    public Type ClrType => typeof(object[]);

    /// <summary>
    /// Gets the number of fields in the nested structure.
    /// </summary>
    public int FieldCount => _fieldReaders.Length;

    /// <summary>
    /// Gets the field names.
    /// </summary>
    public IReadOnlyList<string> FieldNames => _fieldNames;

    /// <summary>
    /// Gets the index of a field by name. Returns -1 if not found.
    /// </summary>
    public int GetFieldIndex(string fieldName)
    {
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
        var nested = new object[_fieldReaders.Length];
        for (int i = 0; i < _fieldReaders.Length; i++)
        {
            using var col = _fieldReaders[i].ReadTypedColumn(ref reader, 1);
            nested[i] = col.GetValue(0)!;
        }
        return nested;
    }

    /// <inheritdoc />
    public TypedColumn<object[]> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return new TypedColumn<object[]>(Array.Empty<object[]>());

        // Read each field column fully (columnar layout)
        // Each field is an Array, so we get T[][] for each field
        var fieldColumns = new ITypedColumn[_fieldReaders.Length];
        try
        {
            for (int f = 0; f < _fieldReaders.Length; f++)
            {
                fieldColumns[f] = _fieldReaders[f].ReadTypedColumn(ref reader, rowCount);
            }

            // Transpose to row-major: each row is [field1Array, field2Array, ...]
            var result = new object[rowCount][];
            for (int row = 0; row < rowCount; row++)
            {
                result[row] = new object[_fieldReaders.Length];
                for (int f = 0; f < _fieldReaders.Length; f++)
                {
                    result[row][f] = fieldColumns[f].GetValue(row)!;
                }
            }

            return new TypedColumn<object[]>(result);
        }
        finally
        {
            // Dispose all field columns
            foreach (var col in fieldColumns)
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
