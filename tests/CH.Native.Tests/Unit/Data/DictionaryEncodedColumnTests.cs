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
