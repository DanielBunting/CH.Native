using System.Buffers;

namespace CH.Native.Data;

/// <summary>
/// A column that stores array data in a flattened format with offset-based access.
/// Instead of allocating a separate array per row, keeps all elements in one array
/// and uses offsets to slice per-row arrays on demand.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
public sealed class FlattenedArrayColumn<T> : ITypedColumn
{
    private readonly T[] _elements;
    private readonly int[] _offsets;
    private readonly int _rowCount;
    private readonly ArrayPool<T>? _elementsPool;
    private readonly ArrayPool<int>? _offsetsPool;
    private readonly int _elementsLength;
    private bool _disposed;

    /// <summary>
    /// Creates a new flattened array column.
    /// </summary>
    /// <param name="elements">The flattened array of all elements.</param>
    /// <param name="elementsLength">The actual number of elements (may be less than array length if pooled).</param>
    /// <param name="offsets">The cumulative offset array (offset[i] = end position of row i's elements).</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="elementsPool">The pool to return the elements array to, or null if not pooled.</param>
    /// <param name="offsetsPool">The pool to return the offsets array to, or null if not pooled.</param>
    public FlattenedArrayColumn(
        T[] elements,
        int elementsLength,
        int[] offsets,
        int rowCount,
        ArrayPool<T>? elementsPool,
        ArrayPool<int>? offsetsPool)
    {
        _elements = elements;
        _elementsLength = elementsLength;
        _offsets = offsets;
        _rowCount = rowCount;
        _elementsPool = elementsPool;
        _offsetsPool = offsetsPool;
    }

    /// <inheritdoc />
    public Type ElementType => typeof(T[]);

    /// <inheritdoc />
    public int Count => _rowCount;

    /// <summary>
    /// Gets the array at the specified row index.
    /// Note: This allocates a new array on each access. For zero-copy access, use GetRowSpan.
    /// </summary>
    public T[] this[int rowIndex]
    {
        get
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(FlattenedArrayColumn<T>));
            if ((uint)rowIndex >= (uint)_rowCount)
                throw new ArgumentOutOfRangeException(nameof(rowIndex));

            var start = rowIndex == 0 ? 0 : _offsets[rowIndex - 1];
            var end = _offsets[rowIndex];
            var length = end - start;

            if (length == 0)
                return Array.Empty<T>();

            var result = new T[length];
            Array.Copy(_elements, start, result, 0, length);
            return result;
        }
    }

    /// <inheritdoc />
    public object? GetValue(int index) => this[index];

    /// <summary>
    /// Gets a zero-copy span view of the array at the specified row.
    /// The span is only valid until this column is disposed.
    /// </summary>
    public ReadOnlySpan<T> GetRowSpan(int rowIndex)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(FlattenedArrayColumn<T>));
        if ((uint)rowIndex >= (uint)_rowCount)
            throw new ArgumentOutOfRangeException(nameof(rowIndex));

        var start = rowIndex == 0 ? 0 : _offsets[rowIndex - 1];
        var end = _offsets[rowIndex];
        return _elements.AsSpan(start, end - start);
    }

    /// <summary>
    /// Gets the total number of elements across all rows.
    /// </summary>
    public int TotalElements => _elementsLength;

    /// <summary>
    /// Gets all elements as a span for bulk processing.
    /// </summary>
    public ReadOnlySpan<T> AllElements => _elements.AsSpan(0, _elementsLength);

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        _elementsPool?.Return(_elements);
        _offsetsPool?.Return(_offsets);
    }
}
