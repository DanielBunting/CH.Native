using System.Reflection;
using System.Runtime.CompilerServices;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Tuple(T1, T2, ...) values.
/// </summary>
/// <remarks>
/// Wire format: Each element is stored as a separate column (columnar layout).
/// All first elements, then all second elements, etc.
///
/// Returns System.Tuple instances to match the reference ClickHouse driver behavior.
/// Supports named tuples like Tuple(id UInt64, name String) where field names are preserved.
/// </remarks>
public sealed class TupleColumnReader : IColumnReader<object>
{
    private readonly IColumnReader[] _elementReaders;
    private readonly string[]? _fieldNames;
    private readonly Type _tupleType;
    private readonly ConstructorInfo _tupleConstructor;

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

        var elementTypes = elementReaders.Select(r => r.ClrType).ToArray();
        _tupleType = MakeTupleType(elementTypes);
        _tupleConstructor = _tupleType.GetConstructors()[0];
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
    public Type ClrType => typeof(object);

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
    public object ReadValue(ref ProtocolReader reader)
    {
        var elements = new object[_elementReaders.Length];
        for (int i = 0; i < _elementReaders.Length; i++)
        {
            // For single value, we read a 1-row column and get first element
            using var col = _elementReaders[i].ReadTypedColumn(ref reader, 1);
            elements[i] = col.GetValue(0)!;
        }
        return CreateTuple(elements);
    }

    /// <inheritdoc />
    public TypedColumn<object> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return new TypedColumn<object>(Array.Empty<object>());

        // Read each element column fully (columnar layout)
        var elementColumns = new ITypedColumn[_elementReaders.Length];
        try
        {
            for (int e = 0; e < _elementReaders.Length; e++)
            {
                elementColumns[e] = _elementReaders[e].ReadTypedColumn(ref reader, rowCount);
            }

            // Transpose to row-major tuples
            var result = new object[rowCount];
            for (int row = 0; row < rowCount; row++)
            {
                var elements = new object[_elementReaders.Length];
                for (int e = 0; e < _elementReaders.Length; e++)
                {
                    elements[e] = elementColumns[e].GetValue(row)!;
                }
                result[row] = CreateTuple(elements);
            }

            return new TypedColumn<object>(result);
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

    /// <summary>
    /// Creates a System.Tuple instance from the element values.
    /// For arity &gt; 7, creates nested Tuple&lt;T1,...,T7,TRest&gt; following .NET conventions.
    /// </summary>
    private object CreateTuple(object?[] elements)
    {
        if (elements.Length <= 7)
            return _tupleConstructor.Invoke(elements);

        return CreateNestedTuple(_tupleType, elements);
    }

    private static object CreateNestedTuple(Type tupleType, object?[] elements, int startIndex = 0)
    {
        var remaining = elements.Length - startIndex;

        if (remaining <= 7)
        {
            var args = new object?[remaining];
            Array.Copy(elements, startIndex, args, 0, remaining);
            return Activator.CreateInstance(tupleType, args)!;
        }

        // Take first 7, then nest the rest
        var restType = tupleType.GetGenericArguments()[7];
        var restTuple = CreateNestedTuple(restType, elements, startIndex + 7);

        var constructorArgs = new object?[8];
        Array.Copy(elements, startIndex, constructorArgs, 0, 7);
        constructorArgs[7] = restTuple;
        return Activator.CreateInstance(tupleType, constructorArgs)!;
    }

    /// <summary>
    /// Builds the concrete System.Tuple generic type from element CLR types.
    /// For arity &gt; 7, creates nested Tuple&lt;T1,...,T7,TRest&gt;.
    /// </summary>
    private static Type MakeTupleType(Type[] elementTypes)
    {
        if (elementTypes.Length == 0)
            throw new ArgumentException("Tuple requires at least one element type.");

        if (elementTypes.Length <= 7)
        {
            var genericDef = elementTypes.Length switch
            {
                1 => typeof(Tuple<>),
                2 => typeof(Tuple<,>),
                3 => typeof(Tuple<,,>),
                4 => typeof(Tuple<,,,>),
                5 => typeof(Tuple<,,,,>),
                6 => typeof(Tuple<,,,,,>),
                7 => typeof(Tuple<,,,,,,>),
                _ => throw new InvalidOperationException()
            };
            return genericDef.MakeGenericType(elementTypes);
        }

        // For > 7 elements, nest: Tuple<T1,...,T7,Tuple<T8,...>>
        var first7 = elementTypes[..7];
        var rest = elementTypes[7..];
        var restType = MakeTupleType(rest);

        var typeArgs = new Type[8];
        Array.Copy(first7, typeArgs, 7);
        typeArgs[7] = restType;
        return typeof(Tuple<,,,,,,,>).MakeGenericType(typeArgs);
    }
}
