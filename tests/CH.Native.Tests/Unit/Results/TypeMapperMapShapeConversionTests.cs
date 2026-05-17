using CH.Native.Data;
using CH.Native.Results;
using Xunit;

namespace CH.Native.Tests.Unit.Results;

/// <summary>
/// Direct tests for <see cref="TypeMapper{T}.TryConvertMapShape"/> — the three
/// CLR-side conversion branches a typed POCO property can trigger when the
/// reader produced one Map shape but the consumer asked for another.
///
/// These conversions sit downstream of the connection-level entries-shape hint;
/// they only fire when the hint mechanism didn't (or couldn't) pre-select the
/// matching reader.
/// </summary>
public class TypeMapperMapShapeConversionTests
{
    [Fact]
    public void ClickHouseMap_To_Dictionary_CollapsesLastWins()
    {
        var source = new ClickHouseMap<string, int>(new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("a", 2),
            new KeyValuePair<string, int>("b", 3),
        });

        var ok = TypeMapper<object>.TryConvertMapShape(
            source, source.GetType(), typeof(Dictionary<string, int>), out var converted);

        Assert.True(ok);
        var dict = Assert.IsType<Dictionary<string, int>>(converted);
        Assert.Equal(2, dict.Count);
        Assert.Equal(2, dict["a"]); // last-wins
        Assert.Equal(3, dict["b"]);
    }

    [Fact]
    public void Dictionary_To_ClickHouseMap_Wraps_Lossy()
    {
        // The Dictionary→ClickHouseMap path is documented as lossy w.r.t. the wire:
        // duplicates were already collapsed at the reader, so HasDuplicateKeys is false.
        var source = new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 };

        var ok = TypeMapper<object>.TryConvertMapShape(
            source, source.GetType(), typeof(ClickHouseMap<string, int>), out var converted);

        Assert.True(ok);
        var map = Assert.IsType<ClickHouseMap<string, int>>(converted);
        Assert.Equal(2, map.Count);
        Assert.False(map.HasDuplicateKeys);
        Assert.Equal(1, map["a"]);
        Assert.Equal(2, map["b"]);
    }

    [Fact]
    public void ClickHouseMap_To_KeyValuePairArray_PreservesEntries()
    {
        var source = new ClickHouseMap<string, int>(new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("a", 2),
        });

        var ok = TypeMapper<object>.TryConvertMapShape(
            source, source.GetType(), typeof(KeyValuePair<string, int>[]), out var converted);

        Assert.True(ok);
        var arr = Assert.IsType<KeyValuePair<string, int>[]>(converted);
        Assert.Equal(2, arr.Length);
        Assert.Equal(new KeyValuePair<string, int>("a", 1), arr[0]);
        Assert.Equal(new KeyValuePair<string, int>("a", 2), arr[1]);
    }

    [Fact]
    public void MismatchedGenericArguments_ReturnsFalse()
    {
        // Dictionary<string,int> → ClickHouseMap<string,long> shouldn't convert —
        // the conversion only matches when the key/value generic args are identical.
        var source = new Dictionary<string, int> { ["a"] = 1 };

        var ok = TypeMapper<object>.TryConvertMapShape(
            source, source.GetType(), typeof(ClickHouseMap<string, long>), out var converted);

        Assert.False(ok);
        Assert.Null(converted);
    }

    [Fact]
    public void UnrelatedTargetType_ReturnsFalse()
    {
        var source = new ClickHouseMap<string, int>(Array.Empty<KeyValuePair<string, int>>());

        var ok = TypeMapper<object>.TryConvertMapShape(
            source, source.GetType(), typeof(string), out var converted);

        Assert.False(ok);
        Assert.Null(converted);
    }

    [Fact]
    public void NonGenericSourceType_ReturnsFalse()
    {
        // The conversion gate requires the source to be generic — a plain Hashtable
        // shouldn't match even if the target looks Map-shaped.
        var source = new System.Collections.Hashtable { ["a"] = 1 };

        var ok = TypeMapper<object>.TryConvertMapShape(
            source, source.GetType(), typeof(Dictionary<string, int>), out var converted);

        Assert.False(ok);
        Assert.Null(converted);
    }
}
