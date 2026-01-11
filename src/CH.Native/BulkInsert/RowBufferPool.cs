using System.Buffers;

namespace CH.Native.BulkInsert;

/// <summary>
/// Pool for row buffer arrays to reduce allocations during streaming bulk insert.
/// Uses ArrayPool&lt;T&gt;.Shared internally for array storage, with an additional
/// layer of pooling to avoid frequent rent/return cycles.
/// </summary>
/// <typeparam name="T">The row type being pooled.</typeparam>
internal sealed class RowBufferPool<T> where T : class
{
    private readonly int _bufferSize;
    private readonly Stack<T[]> _pool = new();
    private readonly object _lock = new();
    private const int MaxPoolSize = 4;

    /// <summary>
    /// Creates a new row buffer pool.
    /// </summary>
    /// <param name="bufferSize">The size of each buffer in the pool.</param>
    public RowBufferPool(int bufferSize)
    {
        if (bufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(bufferSize), "Buffer size must be positive.");

        _bufferSize = bufferSize;
    }

    /// <summary>
    /// Gets the size of buffers in this pool.
    /// </summary>
    public int BufferSize => _bufferSize;

    /// <summary>
    /// Rents a buffer from the pool. The buffer is guaranteed to have at least
    /// <see cref="BufferSize"/> elements.
    /// </summary>
    /// <returns>A buffer that may be larger than requested.</returns>
    public T[] Rent()
    {
        lock (_lock)
        {
            if (_pool.TryPop(out var buffer))
            {
                return buffer;
            }
        }

        // Use ArrayPool for the underlying allocation
        return ArrayPool<T>.Shared.Rent(_bufferSize);
    }

    /// <summary>
    /// Returns a buffer to the pool. The buffer is cleared to release object references.
    /// </summary>
    /// <param name="buffer">The buffer to return.</param>
    /// <param name="clearLength">The number of elements to clear (optimization to avoid clearing entire array).</param>
    public void Return(T[] buffer, int clearLength = -1)
    {
        if (buffer == null)
            return;

        // Clear references to allow GC
        var lengthToClear = clearLength >= 0 ? Math.Min(clearLength, buffer.Length) : buffer.Length;
        Array.Clear(buffer, 0, lengthToClear);

        lock (_lock)
        {
            if (_pool.Count < MaxPoolSize && buffer.Length >= _bufferSize)
            {
                _pool.Push(buffer);
                return;
            }
        }

        // Pool is full or buffer is wrong size - return to ArrayPool
        ArrayPool<T>.Shared.Return(buffer, clearArray: false); // Already cleared above
    }

    /// <summary>
    /// Clears all pooled buffers, returning them to the underlying ArrayPool.
    /// </summary>
    public void Clear()
    {
        lock (_lock)
        {
            while (_pool.TryPop(out var buffer))
            {
                ArrayPool<T>.Shared.Return(buffer, clearArray: true);
            }
        }
    }
}
