using System.Buffers;
using CH.Native.Data;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Pins <see cref="TypedColumn{T}.IsNull(int)"/>: for non-nullable value-type
/// storage the JIT folds <c>default(T) is not null</c> to a constant and the
/// method returns false without allocating; for <see cref="string"/> and
/// <see cref="System.Nullable{T}"/> it falls through to a default-equality
/// check. Disposed and out-of-range access throw the conventional exceptions.
/// </summary>
public class TypedColumnIsNullTests
{
    [Fact]
    public void IsNull_ValueTypeStorage_AlwaysFalse()
    {
        using var col = new TypedColumn<long>(new long[] { 0, 1, long.MaxValue, 0 }, length: 4, pool: NoReturnPool<long>.Instance);

        // Even the slot holding 0 (default(long)) is not "null" — value types can't be null.
        Assert.False(col.IsNull(0));
        Assert.False(col.IsNull(2));
    }

    [Fact]
    public void IsNull_ReferenceTypeStorage_TrueOnlyForNullSlots()
    {
        using var col = new TypedColumn<string?>(new[] { "a", null, "", null }, length: 4, pool: NoReturnPool<string?>.Instance);

        Assert.False(col.IsNull(0));
        Assert.True(col.IsNull(1));
        Assert.False(col.IsNull(2)); // empty string is not null
        Assert.True(col.IsNull(3));
    }

    [Fact]
    public void IsNull_NullableValueTypeStorage_TrueForNullSlots()
    {
        using var col = new TypedColumn<int?>(new int?[] { 5, null, 0, null }, length: 4, pool: NoReturnPool<int?>.Instance);

        Assert.False(col.IsNull(0));
        Assert.True(col.IsNull(1));
        Assert.False(col.IsNull(2)); // 0 is a present value, not null
        Assert.True(col.IsNull(3));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(4)]
    [InlineData(100)]
    public void IsNull_IndexOutOfRange_Throws(int index)
    {
        using var col = new TypedColumn<long>(new long[] { 1, 2, 3, 4 }, length: 4, pool: NoReturnPool<long>.Instance);

        Assert.Throws<ArgumentOutOfRangeException>(() => col.IsNull(index));
    }

    [Fact]
    public void IsNull_AfterDispose_ThrowsObjectDisposed()
    {
        var col = new TypedColumn<long>(new long[] { 1, 2, 3, 4 }, length: 4, pool: NoReturnPool<long>.Instance);
        col.Dispose();

        Assert.Throws<ObjectDisposedException>(() => col.IsNull(0));
    }

    // Pool that hands back fresh arrays and ignores returns, so the test owns
    // the exact backing array and Dispose never trips ArrayPool.Shared's
    // foreign-buffer guard.
    private sealed class NoReturnPool<T> : ArrayPool<T>
    {
        public static readonly NoReturnPool<T> Instance = new();
        public override T[] Rent(int minimumLength) => new T[minimumLength];
        public override void Return(T[] array, bool clearArray = false) { }
    }
}
