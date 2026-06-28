using System.Collections;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Nested(field1 Type1, field2 Type2, ...) values — the write-side
/// counterpart of <see cref="ColumnReaders.NestedColumnReader"/>.
/// </summary>
/// <remarks>
/// Wire format (verified against the server, <c>flatten_nested=0</c>): a Nested column
/// is a set of parallel arrays that <b>share a single offsets block</b>. ClickHouse
/// writes the cumulative row offsets <b>once</b> (one <c>UInt64</c> per row), then each
/// field's element values flattened across all rows — NOT a self-contained
/// <c>Array(fieldType)</c> (with its own offsets) per field. All fields of a row must
/// therefore have the same length.
/// <para>
/// The per-row value is an <c>object[]</c> whose element <c>f</c> is field <c>f</c>'s
/// array for that row (e.g. <c>[ string[] keys, int[] values ]</c> for
/// <c>Nested(key String, value Int32)</c>), matching the row shape produced by
/// <see cref="ColumnReaders.NestedColumnReader"/>.
/// </para>
/// </remarks>
internal sealed class NestedColumnWriter : IColumnWriter<object[]>
{
    // Element writers for each field's INNER type (e.g. String, Int32) — not Array(...)
    // writers: the offsets are shared and written here, once, by this writer.
    private readonly IColumnWriter[] _fieldElementWriters;
    private readonly string[] _fieldNames;

    /// <summary>
    /// Creates a Nested writer from the per-field element writers and names.
    /// </summary>
    /// <param name="fieldElementWriters">Element writers for each field's inner type, in order.</param>
    /// <param name="fieldNames">Names for each field; must match <paramref name="fieldElementWriters"/>.</param>
    public NestedColumnWriter(IColumnWriter[] fieldElementWriters, string[] fieldNames)
    {
        if (fieldElementWriters == null || fieldElementWriters.Length == 0)
            throw new ArgumentException("Nested requires at least one field writer.", nameof(fieldElementWriters));

        if (fieldNames == null || fieldNames.Length != fieldElementWriters.Length)
            throw new ArgumentException("Field names count must match field writers count.", nameof(fieldNames));

        _fieldElementWriters = fieldElementWriters;
        _fieldNames = fieldNames;
    }

    /// <inheritdoc />
    public string TypeName
    {
        get
        {
            var fields = _fieldElementWriters.Select((w, i) => $"{_fieldNames[i]} {w.TypeName}");
            return $"Nested({string.Join(", ", fields)})";
        }
    }

    /// <inheritdoc />
    public Type ClrType => typeof(object[]);

    /// <summary>Gets the number of fields in the nested structure.</summary>
    public int FieldCount => _fieldElementWriters.Length;

    /// <summary>Gets the field names.</summary>
    public IReadOnlyList<string> FieldNames => _fieldNames;

    /// <inheritdoc />
    // Emit each field element-writer's prefix in field order (e.g. a Nested field of
    // LowCardinality(T) must write its KeysSerializationVersion before the data).
    public void WritePrefix(ref ProtocolWriter writer)
    {
        for (int i = 0; i < _fieldElementWriters.Length; i++)
            _fieldElementWriters[i].WritePrefix(ref writer);
    }

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, object[][] values)
    {
        // Step 1: one shared offsets block (UInt64 cumulative) derived from the row
        // lengths. Every field of a row must have the same length.
        ulong cumulative = 0;
        var rowLengths = new int[values.Length];
        for (int row = 0; row < values.Length; row++)
        {
            var fields = values[row];
            if (fields is null)
                throw NullAt(row);
            if (fields.Length != _fieldElementWriters.Length)
                throw ArityMismatch(fields.Length, row);

            int len = FieldLength(fields[0], row, fieldIndex: 0);
            for (int f = 1; f < fields.Length; f++)
            {
                int other = FieldLength(fields[f], row, fieldIndex: f);
                if (other != len)
                    throw Ragged(row, len, f, other);
            }

            rowLengths[row] = len;
            cumulative += (ulong)len;
            writer.WriteUInt64(cumulative);
        }

        // Step 2: each field's element values, flattened across all rows, written with
        // the field's element writer (no per-field offsets).
        for (int f = 0; f < _fieldElementWriters.Length; f++)
        {
            int total = 0;
            for (int row = 0; row < values.Length; row++)
                total += rowLengths[row];

            var flat = new object?[total];
            int pos = 0;
            for (int row = 0; row < values.Length; row++)
            {
                if (values[row][f] is IList list)
                {
                    for (int j = 0; j < list.Count; j++)
                        flat[pos++] = list[j];
                }
            }

            _fieldElementWriters[f].WriteColumn(ref writer, flat);
        }
    }

    /// <inheritdoc />
    // A single Nested value (one row): its offset, then the row's field elements. Used
    // only if a Nested ever appears as an element of another composite.
    public void WriteValue(ref ProtocolWriter writer, object[] value)
    {
        if (value is null)
            throw NullAt(rowIndex: -1);
        if (value.Length != _fieldElementWriters.Length)
            throw ArityMismatch(value.Length, rowIndex: -1);

        int len = FieldLength(value[0], rowIndex: -1, fieldIndex: 0);
        for (int f = 1; f < value.Length; f++)
        {
            int other = FieldLength(value[f], rowIndex: -1, fieldIndex: f);
            if (other != len)
                throw Ragged(rowIndex: -1, len, f, other);
        }

        writer.WriteUInt64((ulong)len);
        for (int f = 0; f < _fieldElementWriters.Length; f++)
        {
            var elementWriter = _fieldElementWriters[f];
            if (value[f] is IList list)
            {
                for (int j = 0; j < list.Count; j++)
                    elementWriter.WriteValue(ref writer, list[j]);
            }
        }
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        var rows = new object[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is null)
                throw NullAt(i);
            rows[i] = ExtractFields(values[i], rowIndex: i);
        }
        WriteColumn(ref writer, rows);
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        if (value is null)
            throw NullAt(rowIndex: -1);
        WriteValue(ref writer, ExtractFields(value, rowIndex: -1));
    }

    private static object[] ExtractFields(object? value, int rowIndex)
    {
        if (value is object[] array)
            return array;

        var where = rowIndex >= 0 ? $" at row {rowIndex}" : string.Empty;
        throw new InvalidOperationException(
            $"NestedColumnWriter received unsupported value type {value!.GetType().Name}{where}. " +
            $"Expected object[] whose elements are the per-field arrays.");
    }

    private int FieldLength(object? fieldValue, int rowIndex, int fieldIndex)
    {
        if (fieldValue is IList list)
            return list.Count;

        var where = rowIndex >= 0 ? $" at row {rowIndex}" : string.Empty;
        throw new InvalidOperationException(
            $"NestedColumnWriter field '{_fieldNames[fieldIndex]}'{where} is " +
            $"{(fieldValue is null ? "null" : fieldValue.GetType().Name)}; each field must be an " +
            $"array (e.g. string[], int[]) of the field's values.");
    }

    private InvalidOperationException Ragged(int rowIndex, int expectedLen, int fieldIndex, int actualLen)
    {
        var where = rowIndex >= 0 ? $" at row {rowIndex}" : string.Empty;
        return new InvalidOperationException(
            $"Nested value{where} is ragged: field '{_fieldNames[0]}' has {expectedLen} elements but " +
            $"field '{_fieldNames[fieldIndex]}' has {actualLen}. All fields of a Nested row must have " +
            $"the same length (they share one offsets block).");
    }

    private InvalidOperationException ArityMismatch(int actual, int rowIndex)
    {
        var where = rowIndex >= 0 ? $" at row {rowIndex}" : string.Empty;
        return new InvalidOperationException(
            $"Nested value{where} has {actual} fields but the column declares " +
            $"{_fieldElementWriters.Length} ({TypeName}); field count must match exactly.");
    }

    private InvalidOperationException NullAt(int rowIndex)
    {
        var where = rowIndex >= 0 ? $" at row {rowIndex}" : string.Empty;
        return new InvalidOperationException(
            $"NestedColumnWriter received null{where}. Nested columns are non-nullable; " +
            $"provide an object[] of per-field arrays (empty arrays for an empty row).");
    }
}
