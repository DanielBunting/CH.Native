using System.Buffers;

namespace CH.Native.Data;

/// <summary>
/// Non-generic interface for typed column storage.
/// Allows heterogeneous column collections while preserving type information.
/// </summary>
public interface ITypedColumn : IDisposable
{
    /// <summary>
    /// The CLR type of elements in this column.
    /// </summary>
    Type ElementType { get; }

    /// <summary>
    /// The number of elements in this column.
    /// </summary>
    int Count { get; }

    /// <summary>
    /// Gets a value at the specified index (boxes value types).
    /// Use typed access via TypedColumn&lt;T&gt; when possible.
    /// </summary>
    object? GetValue(int index);
}

/// <summary>
/// Generic typed column storage - no boxing for value types when accessed via indexer.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
public sealed class TypedColumn<T> : ITypedColumn
{
    private T[]? _values;
    private readonly ArrayPool<T>? _pool;
    private readonly int _length;

    /// <summary>
    /// Creates a new typed column from the given array.
    /// </summary>
    /// <param name="values">The values array.</param>
    /// <param name="length">The actual number of values (may be less than array length for pooled arrays).</param>
    /// <param name="pool">Optional array pool to return the array to on dispose.</param>
    public TypedColumn(T[] values, int? length = null, ArrayPool<T>? pool = null)
    {
        _values = values;
        _length = length ?? values.Length;
        _pool = pool;
    }

    /// <inheritdoc />
    public Type ElementType => typeof(T);

    /// <inheritdoc />
    public int Count => _length;

    /// <summary>
    /// Gets the values as a span for efficient iteration.
    /// </summary>
    public ReadOnlySpan<T> Values => _values.AsSpan(0, _length);

    /// <summary>
    /// Gets the value at the specified index without boxing.
    /// </summary>
    public T this[int index]
    {
        get
        {
            if (_values == null)
                throw new ObjectDisposedException(nameof(TypedColumn<T>));
            if ((uint)index >= (uint)_length)
                throw new ArgumentOutOfRangeException(nameof(index));
            return _values[index];
        }
    }

    /// <inheritdoc />
    public object? GetValue(int index)
    {
        if (_values == null)
            throw new ObjectDisposedException(nameof(TypedColumn<T>));
        if ((uint)index >= (uint)_length)
            throw new ArgumentOutOfRangeException(nameof(index));
        return _values[index];
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_pool != null && _values != null)
        {
            _pool.Return(_values);
        }
        _values = null;
    }
}
