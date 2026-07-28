using System.Buffers;
using CH.Native.Data;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Tests for DictionaryEncodedColumn, focusing on bounds-checking behavior when the
/// dictionary index on the wire is out of range for the dictionary array.
/// </summary>
public class DictionaryEncodedColumnTests
{
    [Fact]
    public void Indexer_DictIndexOutOfRange_ThrowsInvalidDataWithContext()
    {
        // Dictionary has 2 entries; indices contain a bogus 99 for row 0.
        var dictionary = new[] { "a", "b" };
        var indices = ArrayPool<int>.Shared.Rent(1);
        indices[0] = 99;

        using var column = new DictionaryEncodedColumn<string>(
            dictionary,
            indices,
            count: 1,
            indicesPool: ArrayPool<int>.Shared);

        var ex = Assert.Throws<InvalidDataException>(() => _ = column[0]);
        Assert.Contains("99", ex.Message);
        Assert.Contains("2", ex.Message);
    }

    [Fact]
    public void Indexer_DictIndexNegative_ThrowsInvalidData()
    {
        // Even though the wire is unsigned, after the checked-cast path an index can
        // arrive negative if an oversize UInt32 overflowed the cast upstream.
        var dictionary = new[] { "a" };
        var indices = ArrayPool<int>.Shared.Rent(1);
        indices[0] = -1;

        using var column = new DictionaryEncodedColumn<string>(
            dictionary,
            indices,
            count: 1,
            indicesPool: ArrayPool<int>.Shared);

        Assert.Throws<InvalidDataException>(() => _ = column[0]);
    }

    [Fact]
    public void Indexer_EmptyDictionary_ThrowsInvalidData()
    {
        // Malformed LowCardinality stream could arrive with empty dict but non-zero indices.
        var dictionary = Array.Empty<string>();
        var indices = ArrayPool<int>.Shared.Rent(1);
        indices[0] = 0;

        using var column = new DictionaryEncodedColumn<string>(
            dictionary,
            indices,
            count: 1,
            indicesPool: ArrayPool<int>.Shared);

        Assert.Throws<InvalidDataException>(() => _ = column[0]);
    }

    [Fact]
    public void Indexer_ValidIndex_ReturnsDictionaryValue()
    {
        var dictionary = new[] { "alpha", "beta", "gamma" };
        var indices = ArrayPool<int>.Shared.Rent(3);
        indices[0] = 2;
        indices[1] = 0;
        indices[2] = 1;

        using var column = new DictionaryEncodedColumn<string>(
            dictionary,
            indices,
            count: 3,
            indicesPool: ArrayPool<int>.Shared);

        Assert.Equal("gamma", column[0]);
        Assert.Equal("alpha", column[1]);
        Assert.Equal("beta", column[2]);
    }

    [Fact]
    public void Indexer_NullableWithIndexZero_ReturnsDefault()
    {
        // LowCardinality(Nullable(T)) reserves index 0 for null.
        var dictionary = new[] { "", "actual" };
        var indices = ArrayPool<int>.Shared.Rent(2);
        indices[0] = 0;
        indices[1] = 1;

        using var column = new DictionaryEncodedColumn<string>(
            dictionary,
            indices,
            count: 2,
            indicesPool: ArrayPool<int>.Shared,
            isNullable: true);

        Assert.Null(column[0]);
        Assert.Equal("actual", column[1]);
    }

    [Fact]
    public void ValueType_NullableWithIndexZero_IsNullTrue_GetValueNull()
    {
        // LowCardinality(Nullable(Int64)) — index 0 is the null sentinel and
        // the null dictionary slot holds default(long)=0. Regression: without an
        // IsNull override, ITypedColumn.IsNull falls back to `GetValue(i) is null`,
        // and GetValue returned the boxed 0 — so a genuine NULL reported IsNull=false
        // and materialised as 0 (a long? property got 0 instead of null).
        var dictionary = new long[] { 0L, 42L }; // slot 0 = null placeholder, slot 1 = real value
        var indices = ArrayPool<int>.Shared.Rent(3);
        indices[0] = 0; // null
        indices[1] = 1; // 42
        indices[2] = 0; // null again

        using var column = new DictionaryEncodedColumn<long>(
            dictionary,
            indices,
            count: 3,
            indicesPool: ArrayPool<int>.Shared,
            isNullable: true);

        Assert.True(column.IsNull(0));
        Assert.Null(column.GetValue(0));
        Assert.False(column.IsNull(1));
        Assert.Equal(42L, column.GetValue(1));
        Assert.True(column.IsNull(2));
        Assert.Null(column.GetValue(2));
    }

    [Fact]
    public void ValueType_NonNullable_RealZero_IsNotNull()
    {
        // A non-nullable LowCardinality(Int64) column whose value is genuinely 0
        // must NOT be reported as null — the null sentinel only applies when
        // isNullable is set.
        var dictionary = new long[] { 0L };
        var indices = ArrayPool<int>.Shared.Rent(1);
        indices[0] = 0;

        using var column = new DictionaryEncodedColumn<long>(
            dictionary,
            indices,
            count: 1,
            indicesPool: ArrayPool<int>.Shared,
            isNullable: false);

        Assert.False(column.IsNull(0));
        Assert.Equal(0L, column.GetValue(0));
    }

    /// <summary>
    /// GetValue and IsNull are separate overrides from the typed indexer (added so a
    /// value-type NULL isn't masked by the boxed default(T)), so they carry their own
    /// copies of the disposed / bounds guards. Untested, a missing guard there means a
    /// use-after-Dispose reads a returned pool buffer — another consumer's data,
    /// silently, with no exception.
    /// </summary>
    [Fact]
    public void GetValueAndIsNull_AfterDispose_ThrowObjectDisposed()
    {
        var indices = ArrayPool<int>.Shared.Rent(1);
        indices[0] = 0;
        var column = new DictionaryEncodedColumn<long>(
            new long[] { 7L },
            indices,
            count: 1,
            indicesPool: ArrayPool<int>.Shared);

        column.Dispose();

        Assert.Throws<ObjectDisposedException>(() => column.GetValue(0));
        Assert.Throws<ObjectDisposedException>(() => column.IsNull(0));
        Assert.Throws<ObjectDisposedException>(() => _ = column[0]);

        // Dispose is idempotent — a second call must not double-return the rental.
        column.Dispose();
    }

    [Fact]
    public void GetValueAndIsNull_OutOfRange_ThrowArgumentOutOfRange()
    {
        var indices = ArrayPool<int>.Shared.Rent(2);
        indices[0] = 0;

        using var column = new DictionaryEncodedColumn<long>(
            new long[] { 7L },
            indices,
            // count is 1 even though the rental is 2 — the guard must key off count,
            // not the (over-sized) pooled array's length, or a read past the row
            // count returns pool slop as if it were data.
            count: 1,
            indicesPool: ArrayPool<int>.Shared);

        Assert.Throws<ArgumentOutOfRangeException>(() => column.GetValue(1));
        Assert.Throws<ArgumentOutOfRangeException>(() => column.GetValue(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => column.IsNull(1));
        Assert.Throws<ArgumentOutOfRangeException>(() => column.IsNull(-1));
    }

    [Fact]
    public void AdvancedConsumerSurface_ExposesDictionaryAndIndices()
    {
        var indices = ArrayPool<int>.Shared.Rent(4);
        indices[0] = 1;
        indices[1] = 0;

        using var column = new DictionaryEncodedColumn<long>(
            new long[] { 7L, 9L },
            indices,
            count: 2,
            indicesPool: ArrayPool<int>.Shared);

        Assert.Equal(typeof(long), column.ElementType);
        Assert.Equal(2, column.Count);
        Assert.Equal(2, column.DictionarySize);
        Assert.Equal(new long[] { 7L, 9L }, column.Dictionary.ToArray());
        // Indices is sliced to Count, not to the pooled array's length.
        Assert.Equal(new[] { 1, 0 }, column.Indices.ToArray());
    }

    [Fact]
    public void Indexer_OutOfRangeCount_ThrowsArgumentOutOfRange()
    {
        var dictionary = new[] { "a" };
        var indices = ArrayPool<int>.Shared.Rent(1);
        indices[0] = 0;

        using var column = new DictionaryEncodedColumn<string>(
            dictionary,
            indices,
            count: 1,
            indicesPool: ArrayPool<int>.Shared);

        Assert.Throws<ArgumentOutOfRangeException>(() => _ = column[1]);
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = column[-1]);
    }
}
