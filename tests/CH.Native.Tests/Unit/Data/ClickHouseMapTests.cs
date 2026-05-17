using CH.Native.Data;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Tests for <see cref="ClickHouseMap{TKey, TValue}"/>, the lossless materialisation
/// of ClickHouse <c>Map(K, V)</c> columns. ClickHouse permits duplicate keys; this
/// type preserves them. Lookups are first-wins (predictable, matches documented
/// server semantics for <c>m[k]</c>).
/// </summary>
public class ClickHouseMapTests
{
    [Fact]
    public void Ctor_FromArray_PreservesOrderAndCount()
    {
        var entries = new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("b", 2),
            new KeyValuePair<string, int>("c", 3),
        };

        var map = new ClickHouseMap<string, int>(entries);

        Assert.Equal(3, map.Count);
        Assert.Equal(new KeyValuePair<string, int>("a", 1), map[0]);
        Assert.Equal(new KeyValuePair<string, int>("b", 2), map[1]);
        Assert.Equal(new KeyValuePair<string, int>("c", 3), map[2]);
    }

    [Fact]
    public void Ctor_FromEnumerable_MaterialisesExactlyOnce()
    {
        // Tracking enumerable: enumerating twice would cause Count to disagree.
        var enumerations = 0;
        IEnumerable<KeyValuePair<string, int>> Source()
        {
            enumerations++;
            yield return new KeyValuePair<string, int>("a", 1);
            yield return new KeyValuePair<string, int>("b", 2);
        }

        var map = new ClickHouseMap<string, int>(Source());

        Assert.Equal(2, map.Count);
        Assert.Equal(1, enumerations);

        // Subsequent reads of the map must not re-enumerate the source.
        _ = map[0];
        _ = map[1];
        Assert.Equal(1, enumerations);
    }

    [Fact]
    public void Indexer_OnDuplicateKeys_IsFirstWins()
    {
        var entries = new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("a", 2),
            new KeyValuePair<string, int>("b", 3),
        };

        var map = new ClickHouseMap<string, int>(entries);

        Assert.Equal(1, map["a"]);
        Assert.Equal(3, map["b"]);
    }

    [Fact]
    public void TryGetValue_OnDuplicateKeys_IsFirstWins()
    {
        var entries = new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("a", 2),
        };

        var map = new ClickHouseMap<string, int>(entries);

        Assert.True(map.TryGetValue("a", out var value));
        Assert.Equal(1, value);
        Assert.False(map.TryGetValue("missing", out _));
    }

    [Fact]
    public void Keys_Values_EnumerateAllEntriesIncludingDuplicates()
    {
        var entries = new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("a", 2),
            new KeyValuePair<string, int>("b", 3),
        };

        var map = new ClickHouseMap<string, int>(entries);

        Assert.Equal(new[] { "a", "a", "b" }, map.Keys.ToArray());
        Assert.Equal(new[] { 1, 2, 3 }, map.Values.ToArray());
    }

    [Fact]
    public void IReadOnlyDictionaryCount_MatchesIReadOnlyListCount()
    {
        var entries = new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("a", 2),
        };

        var map = new ClickHouseMap<string, int>(entries);

        var asDict = (IReadOnlyDictionary<string, int>)map;
        var asList = (IReadOnlyList<KeyValuePair<string, int>>)map;

        Assert.Equal(2, asDict.Count);
        Assert.Equal(2, asList.Count);
    }

    [Fact]
    public void ToDictionary_IsLastWins()
    {
        var entries = new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("a", 2),
            new KeyValuePair<string, int>("b", 3),
        };

        var map = new ClickHouseMap<string, int>(entries);

        var dict = map.ToDictionary();

        Assert.Equal(2, dict.Count);
        Assert.Equal(2, dict["a"]); // last-wins
        Assert.Equal(3, dict["b"]);
    }

    [Fact]
    public void ToLookup_GroupsDuplicates()
    {
        var entries = new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("a", 2),
            new KeyValuePair<string, int>("b", 3),
        };

        var map = new ClickHouseMap<string, int>(entries);

        var lookup = map.ToLookup();

        Assert.Equal(new[] { 1, 2 }, lookup["a"]);
        Assert.Equal(new[] { 3 }, lookup["b"]);
    }

    [Fact]
    public void HasDuplicateKeys_TrueWhenDuplicate()
    {
        var entries = new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("a", 2),
        };

        var map = new ClickHouseMap<string, int>(entries);

        Assert.True(map.HasDuplicateKeys);
    }

    [Fact]
    public void HasDuplicateKeys_FalseWhenUnique()
    {
        var entries = new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("b", 2),
        };

        var map = new ClickHouseMap<string, int>(entries);

        Assert.False(map.HasDuplicateKeys);
    }

    [Fact]
    public void HasDuplicateKeys_FalseOnEmpty()
    {
        var map = new ClickHouseMap<string, int>(Array.Empty<KeyValuePair<string, int>>());

        Assert.False(map.HasDuplicateKeys);
        Assert.Empty(map);
    }

    [Fact]
    public void AsSpan_LengthMatchesCount()
    {
        var entries = new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("a", 2),
            new KeyValuePair<string, int>("b", 3),
        };

        var map = new ClickHouseMap<string, int>(entries);

        var span = map.AsSpan();

        Assert.Equal(3, span.Length);
        Assert.Equal("a", span[0].Key);
        Assert.Equal(1, span[0].Value);
        Assert.Equal("a", span[1].Key);
        Assert.Equal(2, span[1].Value);
    }

    [Fact]
    public void ContainsKey_OnDuplicateKey_ReturnsTrue()
    {
        var entries = new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("a", 2),
        };

        var map = new ClickHouseMap<string, int>(entries);

        Assert.True(map.ContainsKey("a"));
        Assert.False(map.ContainsKey("missing"));
    }

    [Fact]
    public void Enumeration_YieldsEntriesInOrder()
    {
        var entries = new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("a", 2),
            new KeyValuePair<string, int>("b", 3),
        };

        var map = new ClickHouseMap<string, int>(entries);

        var enumerated = map.ToArray();

        Assert.Equal(entries, enumerated);
    }

    [Fact]
    public void Indexer_MissingKey_ThrowsKeyNotFound()
    {
        var map = new ClickHouseMap<string, int>(Array.Empty<KeyValuePair<string, int>>());

        Assert.Throws<KeyNotFoundException>(() => _ = map["missing"]);
    }
}
