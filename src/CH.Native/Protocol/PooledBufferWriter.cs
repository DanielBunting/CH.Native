using System.Buffers;

namespace CH.Native.Protocol;

/// <summary>
/// A pooled buffer writer that uses ArrayPool to reduce allocations.
/// Thread-safe for single-writer scenarios.
/// </summary>
public sealed class PooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private const int DefaultInitialCapacity = 256;
    private const int MaxArrayPoolSize = 1024 * 1024; // 1MB - larger arrays bypass the pool

    private byte[] _buffer;
    private int _index;
    private bool _disposed;

    /// <summary>
    /// Creates a new pooled buffer writer with the specified initial capacity.
    /// </summary>
    /// <param name="initialCapacity">Initial buffer capacity. Default is 256 bytes.</param>
    public PooledBufferWriter(int initialCapacity = DefaultInitialCapacity)
    {
        if (initialCapacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(initialCapacity));

        _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
        _index = 0;
    }

    /// <summary>
    /// Gets the data written so far as a span.
    /// </summary>
    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _index);

    /// <summary>
    /// Gets the data written so far as a memory.
    /// </summary>
    public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _index);

    /// <summary>
    /// Gets the number of bytes written.
    /// </summary>
    public int WrittenCount => _index;

    /// <summary>
    /// Gets the total capacity of the current buffer.
    /// </summary>
    public int Capacity => _buffer.Length;

    /// <summary>
    /// Gets the remaining capacity before needing to grow.
    /// </summary>
    public int FreeCapacity => _buffer.Length - _index;

    /// <summary>
    /// Resets the writer for reuse without returning the buffer to the pool.
    /// </summary>
    public void Reset()
    {
        _index = 0;
    }

    /// <summary>
    /// Resets the writer and shrinks the buffer if it has grown too large.
    /// </summary>
    /// <param name="maxRetainedCapacity">Maximum capacity to retain. Larger buffers are returned to the pool.</param>
    public void ResetAndShrink(int maxRetainedCapacity = 64 * 1024)
    {
        _index = 0;
        if (_buffer.Length > maxRetainedCapacity)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = ArrayPool<byte>.Shared.Rent(DefaultInitialCapacity);
        }
    }

    /// <summary>
    /// Ensures the buffer has at least the specified capacity.
    /// Call this before writing to avoid resize allocations during write operations.
    /// </summary>
    /// <param name="capacity">The minimum required capacity.</param>
    public void EnsureCapacity(int capacity)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity));

        if (capacity > _buffer.Length)
        {
            byte[] newBuffer = ArrayPool<byte>.Shared.Rent(capacity);
            _buffer.AsSpan(0, _index).CopyTo(newBuffer);
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = newBuffer;
        }
    }

    /// <inheritdoc />
    public void Advance(int count)
    {
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count));

        if (_index > _buffer.Length - count)
            throw new InvalidOperationException("Cannot advance past the end of the buffer.");

        _index += count;
    }

    /// <inheritdoc />
    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        CheckAndResizeBuffer(sizeHint);
        return _buffer.AsMemory(_index);
    }

    /// <inheritdoc />
    public Span<byte> GetSpan(int sizeHint = 0)
    {
        CheckAndResizeBuffer(sizeHint);
        return _buffer.AsSpan(_index);
    }

    private void CheckAndResizeBuffer(int sizeHint)
    {
        if (sizeHint < 0)
            throw new ArgumentOutOfRangeException(nameof(sizeHint));

        if (sizeHint == 0)
            sizeHint = 1;

        if (sizeHint > FreeCapacity)
        {
            int currentLength = _buffer.Length;
            int growBy = Math.Max(sizeHint, currentLength);
            int newSize = checked(currentLength + growBy);

            // Get new buffer from pool
            byte[] newBuffer = ArrayPool<byte>.Shared.Rent(newSize);

            // Copy existing data
            _buffer.AsSpan(0, _index).CopyTo(newBuffer);

            // Return old buffer to pool
            ArrayPool<byte>.Shared.Return(_buffer);

            _buffer = newBuffer;
        }
    }

    /// <summary>
    /// Copies the written data to a new byte array.
    /// </summary>
    public byte[] ToArray()
    {
        return WrittenSpan.ToArray();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = Array.Empty<byte>();
        }
    }
}

/// <summary>
/// Pool for PooledBufferWriter instances to reduce allocations in high-throughput scenarios.
/// </summary>
public sealed class BufferWriterPool
{
    private readonly int _initialCapacity;
    private readonly int _maxRetainedCapacity;
    private readonly Stack<PooledBufferWriter> _pool = new();
    private readonly object _lock = new();

    /// <summary>
    /// Gets the default shared pool instance.
    /// </summary>
    public static BufferWriterPool Shared { get; } = new(initialCapacity: 4096, maxRetainedCapacity: 64 * 1024);

    /// <summary>
    /// Creates a new buffer writer pool.
    /// </summary>
    /// <param name="initialCapacity">Initial capacity for new writers.</param>
    /// <param name="maxRetainedCapacity">Maximum buffer size to retain when returning to pool.</param>
    public BufferWriterPool(int initialCapacity = 4096, int maxRetainedCapacity = 64 * 1024)
    {
        _initialCapacity = initialCapacity;
        _maxRetainedCapacity = maxRetainedCapacity;
    }

    /// <summary>
    /// Rents a buffer writer from the pool.
    /// </summary>
    public PooledBufferWriter Rent()
    {
        lock (_lock)
        {
            if (_pool.TryPop(out var writer))
            {
                writer.Reset();
                return writer;
            }
        }

        return new PooledBufferWriter(_initialCapacity);
    }

    /// <summary>
    /// Rents a buffer writer from the pool with a minimum capacity hint.
    /// This avoids resize allocations during write operations for large payloads.
    /// </summary>
    /// <param name="sizeHint">Estimated size needed. The buffer will be at least this size.</param>
    public PooledBufferWriter Rent(int sizeHint)
    {
        PooledBufferWriter? writer = null;

        lock (_lock)
        {
            if (_pool.TryPop(out writer))
            {
                writer.Reset();
            }
        }

        if (writer == null)
        {
            // Create with the larger of initial capacity or size hint
            var capacity = Math.Max(_initialCapacity, sizeHint);
            return new PooledBufferWriter(capacity);
        }

        // Ensure the pooled writer has enough capacity
        if (sizeHint > 0)
        {
            writer.EnsureCapacity(sizeHint);
        }

        return writer;
    }

    /// <summary>
    /// Returns a buffer writer to the pool.
    /// </summary>
    public void Return(PooledBufferWriter writer)
    {
        writer.ResetAndShrink(_maxRetainedCapacity);

        lock (_lock)
        {
            // Limit pool size to prevent unbounded growth
            if (_pool.Count < 16)
            {
                _pool.Push(writer);
            }
            else
            {
                writer.Dispose();
            }
        }
    }
}
