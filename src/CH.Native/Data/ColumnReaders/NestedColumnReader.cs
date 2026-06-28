using CH.Native.Exceptions;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Nested(field1 Type1, field2 Type2, ...) values — the read-side
/// counterpart of <see cref="ColumnWriters.NestedColumnWriter"/>.
/// </summary>
/// <remarks>
/// Wire format (verified against the server): a Nested column is a set of parallel
/// arrays that <b>share a single offsets block</b>. The cumulative row offsets are sent
/// <b>once</b> (one <c>UInt64</c> per row), then each field's element values flattened
/// across all rows — NOT a self-contained <c>Array(fieldType)</c> (with its own offsets)
/// per field. So the reader reads the offsets once and slices every field's flat values
/// with them.
/// <para>
/// Returns <c>object[][]</c> where each row is <c>[ field1Array, field2Array, ... ]</c>;
/// each field value is a typed array (e.g. <c>string[]</c>, <c>int[]</c>) of the field's
/// values for that row.
/// </para>
/// </remarks>
internal sealed class NestedColumnReader : IColumnReader<object[]>
{
    // Element readers for each field's INNER type (e.g. String, Int32) — not Array(...)
    // readers: the offsets are shared and read here, once.
    private readonly IColumnReader[] _fieldElementReaders;
    private readonly string[] _fieldNames;

    /// <summary>
    /// Creates a Nested reader from the per-field element readers and names.
    /// </summary>
    /// <param name="fieldElementReaders">Element readers for each field's inner type, in order.</param>
    /// <param name="fieldNames">Names for each field; must match <paramref name="fieldElementReaders"/>.</param>
    public NestedColumnReader(IColumnReader[] fieldElementReaders, string[] fieldNames)
    {
        if (fieldElementReaders == null || fieldElementReaders.Length == 0)
            throw new ArgumentException("Nested requires at least one field reader.", nameof(fieldElementReaders));

        if (fieldNames == null || fieldNames.Length != fieldElementReaders.Length)
            throw new ArgumentException("Field names count must match field readers count.", nameof(fieldNames));

        _fieldElementReaders = fieldElementReaders;
        _fieldNames = fieldNames;
    }

    /// <inheritdoc />
    public string TypeName
    {
        get
        {
            var fields = _fieldElementReaders.Select((r, i) => $"{_fieldNames[i]} {r.TypeName}");
            return $"Nested({string.Join(", ", fields)})";
        }
    }

    /// <inheritdoc />
    public Type ClrType => typeof(object[]);

    /// <summary>Gets the number of fields in the nested structure.</summary>
    public int FieldCount => _fieldElementReaders.Length;

    /// <summary>Gets the field names.</summary>
    public IReadOnlyList<string> FieldNames => _fieldNames;

    /// <summary>Gets the index of a field by name. Returns -1 if not found.</summary>
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
    // Read each field element-reader's prefix in field order, matching
    // NestedColumnWriter.WritePrefix (e.g. a LowCardinality field's version byte).
    public void ReadPrefix(ref ProtocolReader reader)
    {
        for (int i = 0; i < _fieldElementReaders.Length; i++)
            _fieldElementReaders[i].ReadPrefix(ref reader);
    }

    /// <inheritdoc />
    // A single Nested value (one row): its offset, then the row's field elements.
    public object[] ReadValue(ref ProtocolReader reader)
    {
        var length = reader.ReadUInt64AsInt32("Nested offset");
        var nested = new object[_fieldElementReaders.Length];
        for (int f = 0; f < _fieldElementReaders.Length; f++)
        {
            using var col = _fieldElementReaders[f].ReadTypedColumn(ref reader, length);
            nested[f] = BuildFieldArray(_fieldElementReaders[f].ClrType, col, start: 0, length);
        }
        return nested;
    }

    /// <inheritdoc />
    public TypedColumn<object[]> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return new TypedColumn<object[]>(Array.Empty<object[]>());

        // Step 1: one shared offsets block (UInt64 cumulative), read once.
        var offsets = new int[rowCount];
        int previous = 0;
        for (int i = 0; i < rowCount; i++)
        {
            var offset = reader.ReadUInt64AsInt32("Nested offset");
            if (offset < previous)
            {
                throw new ClickHouseProtocolException(
                    $"Nested offset at row {i} ({offset}) is less than previous cumulative offset " +
                    $"({previous}); offsets must be monotonically non-decreasing.");
            }
            offsets[i] = offset;
            previous = offset;
        }

        var totalElements = offsets[rowCount - 1];

        // Step 2: each field's flat element column, sliced per row using the shared offsets.
        var result = new object[rowCount][];
        for (int row = 0; row < rowCount; row++)
            result[row] = new object[_fieldElementReaders.Length];

        for (int f = 0; f < _fieldElementReaders.Length; f++)
        {
            var clr = _fieldElementReaders[f].ClrType;
            using var elements = _fieldElementReaders[f].ReadTypedColumn(ref reader, totalElements);

            int start = 0;
            for (int row = 0; row < rowCount; row++)
            {
                int end = offsets[row];
                result[row][f] = BuildFieldArray(clr, elements, start, end - start);
                start = end;
            }
        }

        return new TypedColumn<object[]>(result);
    }

    // Builds a typed array (string[], int[], ...) of the field's element CLR type for one
    // row's slice [start, start+length) of the field's flat element column.
    private static Array BuildFieldArray(Type elementClrType, ITypedColumn elements, int start, int length)
    {
        var array = Array.CreateInstance(elementClrType, length);
        for (int j = 0; j < length; j++)
            array.SetValue(elements.GetValue(start + j), j);
        return array;
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
