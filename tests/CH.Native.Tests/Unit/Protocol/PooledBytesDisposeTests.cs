using System.Buffers;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Protocol;

/// <summary>
/// Security regression tests for <see cref="PooledBytes.Dispose"/>. Rented buffers
/// must be returned to the pool in a cleared state — otherwise wire-derived data
/// (strings, payloads, potentially auth text) can leak to unrelated consumers of
/// <see cref="ArrayPool{T}.Shared"/>.
/// </summary>
public class PooledBytesDisposeTests
{
    /// <summary>
    /// ArrayPool wrapper that records every Return call and delegates to an inner
    /// pool. Lets tests observe whether Dispose asked for the buffer to be cleared.
    /// </summary>
    private sealed class TrackingArrayPool<T> : ArrayPool<T>
    {
        private readonly ArrayPool<T> _inner = ArrayPool<T>.Shared;

        public List<(T[] Array, bool ClearArray)> Returns { get; } = new();

        public override T[] Rent(int minimumLength) => _inner.Rent(minimumLength);

        public override void Return(T[] array, bool clearArray = false)
        {
            Returns.Add((array, clearArray));
            _inner.Return(array, clearArray);
        }
    }

    [Fact]
    public void Dispose_ReturnsBufferWithClearArrayTrue()
    {
        var pool = new TrackingArrayPool<byte>();
        var array = pool.Rent(64);
        var pooled = new PooledBytes(array.AsMemory(0, 16), pool, array);

        pooled.Dispose();

        Assert.Single(pool.Returns);
        Assert.True(pool.Returns[0].ClearArray,
            "PooledBytes.Dispose must return the array with clearArray: true to prevent cross-consumer data leaks.");
    }

    [Fact]
    public void Dispose_ZeroesUsedRegionOfBuffer()
    {
        var pool = new TrackingArrayPool<byte>();
        var array = pool.Rent(64);

        // Simulate the fragmented-read path: fill the used region with wire data.
        for (int i = 0; i < 16; i++) array[i] = 0xAA;

        var pooled = new PooledBytes(array.AsMemory(0, 16), pool, array);
        pooled.Dispose();

        // After dispose, the returned array must be zero-filled in the range we wrote.
        for (int i = 0; i < 16; i++)
        {
            Assert.Equal(0, array[i]);
        }
    }

    [Fact]
    public void Dispose_ZeroesTailResidueBeyondUsedRegion()
    {
        var pool = new TrackingArrayPool<byte>();
        var array = pool.Rent(64);

        // Tail (beyond the 16 bytes we'd logically "use") has residue from prior tenant.
        for (int i = 16; i < array.Length; i++) array[i] = 0xBB;

        var pooled = new PooledBytes(array.AsMemory(0, 16), pool, array);
        pooled.Dispose();

        // Tail residue should be zeroed too — clearArray: true on Return clears the
        // entire buffer, not just the slice we exposed.
        for (int i = 16; i < array.Length; i++)
        {
            Assert.Equal(0, array[i]);
        }
    }

    [Fact]
    public void Dispose_WithNoPool_IsNoOp()
    {
        // The single-segment path wraps caller-owned memory without pool involvement.
        // Disposing must not attempt to clear or return anything.
        var memory = new byte[] { 1, 2, 3, 4 }.AsMemory();
        var pooled = new PooledBytes(memory, pool: null, array: null);

        // Should not throw.
        pooled.Dispose();
        pooled.Dispose(); // idempotent

        // Underlying memory is caller-owned and must not be touched.
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, memory.ToArray());
    }

    [Fact]
    public void Empty_Dispose_IsNoOp()
    {
        PooledBytes.Empty.Dispose(); // should not throw
        Assert.Equal(0, PooledBytes.Empty.Length);
    }
}
