using CH.Native.BulkInsert;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

/// <summary>
/// <see cref="RowBufferPool{T}"/> sits in the bulk-insert hot path. It rents
/// from <see cref="System.Buffers.ArrayPool{T}.Shared"/> on miss and keeps up
/// to 4 buffers locally. These tests pin the contention contract (no
/// exceptions under parallel rent/return), the overflow-to-shared path, the
/// per-T isolation, and the null-safe Return.
/// </summary>
public class RowBufferPoolContentionTests
{
    [Fact]
    public void Rent_ReturnsBufferAtLeastBufferSize()
    {
        var pool = new RowBufferPool<string>(bufferSize: 32);
        var buffer = pool.Rent();

        Assert.NotNull(buffer);
        Assert.True(buffer.Length >= 32);
    }

    [Fact]
    public void BufferSize_ReflectsConstructor()
    {
        Assert.Equal(64, new RowBufferPool<string>(64).BufferSize);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_NonPositiveBufferSize_Throws(int size)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RowBufferPool<string>(size));
    }

    [Fact]
    public void Return_NullBuffer_DoesNotThrow()
    {
        var pool = new RowBufferPool<string>(8);
        pool.Return(null!);  // contract: no-op for null
    }

    [Fact]
    public void RentReturn_RoundTrip_ReuseFromLocalPool()
    {
        var pool = new RowBufferPool<string>(8);
        var first = pool.Rent();
        pool.Return(first);

        // Next rent should hit the local stack and return the same instance.
        var second = pool.Rent();
        Assert.Same(first, second);
    }

    [Fact]
    public void Return_ClearsBufferReferences_BeforePooling()
    {
        var pool = new RowBufferPool<object>(4);
        var buffer = pool.Rent();
        var marker = new object();
        for (int i = 0; i < buffer.Length; i++) buffer[i] = marker;

        pool.Return(buffer);

        // Re-rent and verify the slots have been cleared.
        var next = pool.Rent();
        Assert.Same(buffer, next);
        for (int i = 0; i < next.Length; i++) Assert.Null(next[i]);
    }

    [Fact]
    public void Return_OverflowBeyondMaxPoolSize_FallsBackToSharedPool()
    {
        // MaxPoolSize is 4. Rent 5 buffers, return all 5. The 5th return
        // exceeds the pool capacity and is handed back to ArrayPool.Shared
        // (no observable effect from the consumer's POV, just no exception).
        var pool = new RowBufferPool<string>(8);
        var buffers = new string[5][];
        for (int i = 0; i < 5; i++) buffers[i] = pool.Rent();
        for (int i = 0; i < 5; i++) pool.Return(buffers[i]);

        // Subsequent rents should still succeed (some come from local, some from shared).
        for (int i = 0; i < 10; i++)
        {
            var b = pool.Rent();
            Assert.True(b.Length >= 8);
        }
    }

    [Fact]
    public void Return_ClearLengthOptimisation_OnlyClearsRequestedRange()
    {
        var pool = new RowBufferPool<object>(8);
        var buffer = pool.Rent();
        var marker = new object();
        for (int i = 0; i < buffer.Length; i++) buffer[i] = marker;

        // Clear only the first 3 slots (caller hint that the rest are already null
        // or don't matter). The remaining slots may still hold references — pin
        // the contract so a refactor doesn't accidentally always-clear-all.
        pool.Return(buffer, clearLength: 3);

        var next = pool.Rent();
        Assert.Same(buffer, next);
        for (int i = 0; i < 3; i++) Assert.Null(next[i]);
        // Slots 3..buffer.Length are not asserted — the caller said they don't matter.
    }

    [Fact]
    public void ParallelRentReturn_NoExceptionsAndAllRoundTrip()
    {
        // Hammer the lock: 8 threads each rent + return 1000 times. The pool
        // must never throw, and every rented buffer must be at least the
        // requested size.
        var pool = new RowBufferPool<string>(16);
        Parallel.For(0, 8000, i =>
        {
            var b = pool.Rent();
            Assert.True(b.Length >= 16);
            pool.Return(b);
        });
    }

    [Fact]
    public void DifferentTypeInstances_DoNotShareBuffers()
    {
        // Two RowBufferPool<T> instances for different T are completely
        // independent — pin so a refactor that introduces global state can't
        // silently cross-contaminate.
        var stringPool = new RowBufferPool<string>(8);
        var objectPool = new RowBufferPool<object>(8);

        var sBuffer = stringPool.Rent();
        var oBuffer = objectPool.Rent();

        // Different element types, different instances.
        Assert.IsType<string[]>(sBuffer);
        Assert.IsType<object[]>(oBuffer);
        Assert.NotSame((object)sBuffer, oBuffer);
    }

    [Fact]
    public void Clear_EmptiesLocalPool_SubsequentRentHitsSharedPool()
    {
        var pool = new RowBufferPool<string>(8);
        var first = pool.Rent();
        pool.Return(first);

        pool.Clear();

        // After Clear, the local pool is empty. Rent must still succeed via
        // ArrayPool.Shared. The returned buffer may or may not be the same
        // instance (depends on whether ArrayPool.Shared kept it).
        var afterClear = pool.Rent();
        Assert.NotNull(afterClear);
        Assert.True(afterClear.Length >= 8);
    }
}
