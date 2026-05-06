using System.Runtime.CompilerServices;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Tuple(T1, T2, ...) values.
/// </summary>
/// <remarks>
/// Wire format: Each element is stored as a separate column (columnar layout).
/// All first elements, then all second elements, etc.
///
/// Expects object[] because the tuple arity and element types are not known at compile time.
/// Supports named tuples like Tuple(id UInt64, name String) where field names are preserved.
/// </remarks>
internal sealed class TupleColumnWriter : IColumnWriter<object[]>
{
    private readonly IColumnWriter[] _elementWriters;
    private readonly string[]? _fieldNames;

    /// <summary>
    /// Creates a Tuple writer with the specified element writers.
    /// </summary>
    /// <param name="elementWriters">Writers for each tuple element, in order.</param>
    /// <param name="fieldNames">Optional field names for named tuples.</param>
    public TupleColumnWriter(IColumnWriter[] elementWriters, string[]? fieldNames = null)
    {
        if (elementWriters == null || elementWriters.Length == 0)
            throw new ArgumentException("Tuple requires at least one element writer.", nameof(elementWriters));

        if (fieldNames != null && fieldNames.Length != elementWriters.Length)
            throw new ArgumentException("Field names count must match element writers count.", nameof(fieldNames));

        _elementWriters = elementWriters;
        _fieldNames = fieldNames;
    }

    /// <inheritdoc />
    public string TypeName
    {
        get
        {
            if (_fieldNames != null)
            {
                var fields = _elementWriters.Select((w, i) => $"{_fieldNames[i]} {w.TypeName}");
                return $"Tuple({string.Join(", ", fields)})";
            }
            return $"Tuple({string.Join(", ", _elementWriters.Select(w => w.TypeName))})";
        }
    }

    /// <inheritdoc />
    public Type ClrType => typeof(object[]);

    /// <inheritdoc />
    // Nullable(Tuple(...)) wrapper substitutes this empty arity-sized array
    // under a null-bitmap byte. Each inner element writer sees a default
    // slot; the bitmap tells the server to ignore the row.
    public object[] NullPlaceholder => new object[_elementWriters.Length];

    /// <summary>
    /// Gets the number of elements in the tuple.
    /// </summary>
    public int Arity => _elementWriters.Length;

    /// <summary>
    /// Gets the field names for named tuples. Returns null for positional tuples.
    /// </summary>
    public IReadOnlyList<string>? FieldNames => _fieldNames;

    /// <inheritdoc />
    // Emit each element writer's prefix in field order — needed so a Tuple(…, LC(T), …)
    // column's LC KeysSerializationVersion precedes the tuple field bytes.
    public void WritePrefix(ref ProtocolWriter writer)
    {
        for (int i = 0; i < _elementWriters.Length; i++)
        {
            _elementWriters[i].WritePrefix(ref writer);
        }
    }

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, object[][] values)
    {
        // Reject null rows up-front — Tuple(...) is non-nullable; Nullable(Tuple(...))
        // wraps with NullableRefColumnWriter which substitutes an arity-sized array
        // before delegating here. Without this check, inner element writers see null
        // and silently default (zero bytes for numerics — data corruption).
        for (int row = 0; row < values.Length; row++)
        {
            if (values[row] is null)
                throw NullAt(row);
        }

        // Write in columnar layout: all first elements, then all second elements, etc.
        for (int e = 0; e < _elementWriters.Length; e++)
        {
            var elementValues = new object?[values.Length];
            for (int row = 0; row < values.Length; row++)
            {
                elementValues[row] = values[row][e];
            }
            _elementWriters[e].WriteColumn(ref writer, elementValues);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, object[] value)
    {
        if (value is null)
            throw NullAt(rowIndex: -1);

        // Reject arity mismatch loudly. Pre-fix a short array silently passed
        // null to non-nullable inner writers (Int32 etc.), corrupting the
        // wire data; an over-long array silently dropped the trailing
        // elements.
        if (value.Length != _elementWriters.Length)
        {
            throw new InvalidOperationException(
                $"Tuple value has {value.Length} elements but the column declares " +
                $"{_elementWriters.Length} ({TypeName}); arity must match exactly.");
        }

        // Write each element in order.
        for (int i = 0; i < _elementWriters.Length; i++)
        {
            _elementWriters[i].WriteValue(ref writer, value[i]);
        }
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        var tuples = new object[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is null)
                throw NullAt(i);
            tuples[i] = ExtractElements(values[i], rowIndex: i);
        }
        WriteColumn(ref writer, tuples);
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        if (value is null)
            throw NullAt(rowIndex: -1);
        WriteValue(ref writer, ExtractElements(value, rowIndex: -1));
    }

    private static object[] ExtractElements(object? value, int rowIndex)
    {
        if (value is object[] array)
            return array;

        if (value is ITuple tuple)
        {
            var elements = new object[tuple.Length];
            for (int i = 0; i < tuple.Length; i++)
                elements[i] = tuple[i]!;
            return elements;
        }

        var where = rowIndex >= 0 ? $" at row {rowIndex}" : string.Empty;
        throw new InvalidOperationException(
            $"TupleColumnWriter received unsupported value type {value!.GetType().Name}{where}. " +
            $"Expected object[] or ITuple.");
    }

    private static InvalidOperationException NullAt(int rowIndex)
    {
        var where = rowIndex >= 0 ? $" at row {rowIndex}" : string.Empty;
        return new InvalidOperationException(
            $"TupleColumnWriter received null{where}. The Tuple column type " +
            $"is non-nullable; declare the column as Nullable(Tuple(...)) and wrap " +
            $"this writer with NullableRefColumnWriter, or ensure source values are non-null.");
    }
}
