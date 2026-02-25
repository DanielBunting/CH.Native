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
public sealed class TupleColumnWriter : IColumnWriter<object[]>
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

    /// <summary>
    /// Gets the number of elements in the tuple.
    /// </summary>
    public int Arity => _elementWriters.Length;

    /// <summary>
    /// Gets the field names for named tuples. Returns null for positional tuples.
    /// </summary>
    public IReadOnlyList<string>? FieldNames => _fieldNames;

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, object[][] values)
    {
        // Write in columnar layout: all first elements, then all second elements, etc.
        for (int e = 0; e < _elementWriters.Length; e++)
        {
            var elementValues = new object?[values.Length];
            for (int row = 0; row < values.Length; row++)
            {
                elementValues[row] = values[row]?[e];
            }
            _elementWriters[e].WriteColumn(ref writer, elementValues);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, object[] value)
    {
        // Write each element in order
        for (int i = 0; i < _elementWriters.Length; i++)
        {
            _elementWriters[i].WriteValue(ref writer, i < value.Length ? value[i] : null);
        }
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        var tuples = new object[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            tuples[i] = ExtractElements(values[i]);
        }
        WriteColumn(ref writer, tuples);
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        WriteValue(ref writer, ExtractElements(value));
    }

    private static object[] ExtractElements(object? value)
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

        return Array.Empty<object>();
    }
}
