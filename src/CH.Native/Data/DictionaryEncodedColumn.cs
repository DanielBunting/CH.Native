using System.Buffers;

namespace CH.Native.Data;

/// <summary>
/// A column that preserves dictionary encoding for memory efficiency.
/// Instead of expanding dictionary values to a full array, keeps the dictionary
/// and indices separate, resolving values on access.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
public sealed class DictionaryEncodedColumn<T> : ITypedColumn
{
    private readonly T[] _dictionary;
    private readonly int[] _indices;
    private readonly int _count;
    private readonly ArrayPool<int>? _indicesPool;
    private bool _disposed;

    /// <summary>
    /// Creates a new dictionary-encoded column.
    /// </summary>
    /// <param name="dictionary">The dictionary of unique values.</param>
    /// <param name="indices">The indices into the dictionary for each row.</param>
    /// <param name="count">The number of rows (may be less than indices array length if pooled).</param>
    /// <param name="indicesPool">The pool to return the indices array to, or null if not pooled.</param>
    public DictionaryEncodedColumn(T[] dictionary, int[] indices, int count, ArrayPool<int>? indicesPool)
    {
        _dictionary = dictionary;
        _indices = indices;
        _count = count;
        _indicesPool = indicesPool;
    }

    /// <inheritdoc />
    public Type ElementType => typeof(T);

    /// <inheritdoc />
    public int Count => _count;

    /// <summary>
    /// Gets the value at the specified index by resolving from the dictionary.
    /// </summary>
    public T this[int index]
    {
        get
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(DictionaryEncodedColumn<T>));
            if ((uint)index >= (uint)_count)
                throw new ArgumentOutOfRangeException(nameof(index));

            var dictIndex = _indices[index];
            return _dictionary[dictIndex];
        }
    }

    /// <inheritdoc />
    public object? GetValue(int index) => this[index];

    /// <summary>
    /// Gets the dictionary of unique values for advanced consumers.
    /// </summary>
    public ReadOnlySpan<T> Dictionary => _dictionary;

    /// <summary>
    /// Gets the indices into the dictionary for advanced consumers.
    /// </summary>
    public ReadOnlySpan<int> Indices => _indices.AsSpan(0, _count);

    /// <summary>
    /// Gets the number of unique values in the dictionary.
    /// </summary>
    public int DictionarySize => _dictionary.Length;

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        if (_indicesPool != null)
        {
            _indicesPool.Return(_indices);
        }
    }
}
