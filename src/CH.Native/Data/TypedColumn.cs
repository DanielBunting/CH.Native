using System.Buffers;
using System.Runtime.CompilerServices;

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

    /// <summary>
    /// Returns true if the value at <paramref name="index"/> is null.
    /// The default implementation materialises via <see cref="GetValue(int)"/> and
    /// boxes the value for the check; override in subclasses (e.g. <see cref="TypedColumn{T}"/>)
    /// to skip the allocation for non-nullable storage.
    /// </summary>
    bool IsNull(int index) => GetValue(index) is null;
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
    public bool IsNull(int index)
    {
        if (_values == null)
            throw new ObjectDisposedException(nameof(TypedColumn<T>));
        if ((uint)index >= (uint)_length)
            throw new ArgumentOutOfRangeException(nameof(index));

        // Non-nullable value-type storage cannot hold null. The JIT folds
        // `default(T) is not null` to a constant for value types, so this
        // becomes a direct `return false` per specialised T (long, double,
        // DateTime, …). For string and Nullable<T>, fall through to a
        // default-equality check that does not allocate.
        if (default(T) is not null)
            return false;

        return EqualityComparer<T>.Default.Equals(_values[index], default!);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_pool != null && _values != null)
        {
            // Clear references on return so the previous renter's objects don't
            // stay rooted in the pool's freelist; skip the cost when T is a pure
            // value type with no reference fields.
            _pool.Return(_values, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
        }
        _values = null;
    }
}
