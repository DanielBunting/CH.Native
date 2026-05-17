using CH.Native.Data;
using CH.Native.Mapping;
using Xunit;

namespace CH.Native.Tests.Unit.Mapping;

/// <summary>
/// Tests for the internal <see cref="MapShapeInspector"/> that walks a typed row
/// class's properties and decides which <c>Map(K, V)</c> columns should be
/// materialised as <see cref="ClickHouseMap{TKey, TValue}"/>.
/// </summary>
public class MapShapeInspectorTests
{
    private class DictPoco { public Dictionary<string, int> M { get; set; } = new(); }
    private class IDictPoco { public IDictionary<string, int> M { get; set; } = null!; }
    private class IReadOnlyDictPoco { public IReadOnlyDictionary<string, int> M { get; set; } = null!; }
    private class ClickHouseMapPoco { public ClickHouseMap<string, int> M { get; set; } = null!; }
    private class KvpArrayPoco { public KeyValuePair<string, int>[] M { get; set; } = null!; }
    private class IReadOnlyListPoco { public IReadOnlyList<KeyValuePair<string, int>> M { get; set; } = null!; }
    private class IListPoco { public IList<KeyValuePair<string, int>> M { get; set; } = null!; }
    private class NonMapPoco { public int M { get; set; } }
    private class NestedPoco { public List<ClickHouseMap<string, int>> M { get; set; } = null!; }
    private class AttributePoco
    {
        [ClickHouseColumn(Name = "settings")]
        public ClickHouseMap<string, int> Configuration { get; set; } = null!;
    }

    [Fact]
    public void Dictionary_Property_HintsDictionary()
    {
        var hints = MapShapeInspector.Inspect(typeof(DictPoco));
        Assert.Equal(MapShape.Dictionary, hints["M"]);
    }

    [Fact]
    public void IDictionary_Property_HintsDictionary()
    {
        var hints = MapShapeInspector.Inspect(typeof(IDictPoco));
        Assert.Equal(MapShape.Dictionary, hints["M"]);
    }

    [Fact]
    public void IReadOnlyDictionary_Property_HintsDictionary()
    {
        var hints = MapShapeInspector.Inspect(typeof(IReadOnlyDictPoco));
        Assert.Equal(MapShape.Dictionary, hints["M"]);
    }

    [Fact]
    public void ClickHouseMap_Property_HintsEntries()
    {
        var hints = MapShapeInspector.Inspect(typeof(ClickHouseMapPoco));
        Assert.Equal(MapShape.Entries, hints["M"]);
    }

    [Fact]
    public void KeyValuePairArray_Property_HintsEntries()
    {
        var hints = MapShapeInspector.Inspect(typeof(KvpArrayPoco));
        Assert.Equal(MapShape.Entries, hints["M"]);
    }

    [Fact]
    public void IReadOnlyListOfKvp_Property_HintsEntries()
    {
        var hints = MapShapeInspector.Inspect(typeof(IReadOnlyListPoco));
        Assert.Equal(MapShape.Entries, hints["M"]);
    }

    [Fact]
    public void IListOfKvp_Property_HintsEntries()
    {
        var hints = MapShapeInspector.Inspect(typeof(IListPoco));
        Assert.Equal(MapShape.Entries, hints["M"]);
    }

    [Fact]
    public void NonMapProperty_HasNoHint()
    {
        var hints = MapShapeInspector.Inspect(typeof(NonMapPoco));
        Assert.False(hints.ContainsKey("M"));
    }

    [Fact]
    public void NestedClickHouseMapWrappedInList_TopLevelOnly_HasNoHint()
    {
        var hints = MapShapeInspector.Inspect(typeof(NestedPoco));
        Assert.False(hints.ContainsKey("M"));
    }

    [Fact]
    public void Attribute_OverridesPropertyName()
    {
        var hints = MapShapeInspector.Inspect(typeof(AttributePoco));
        Assert.Equal(MapShape.Entries, hints["settings"]);
        Assert.False(hints.ContainsKey("Configuration"));
    }

    [Fact]
    public void ScalarType_ClickHouseMap_HasSentinelStarHint()
    {
        // For scalar (single-column) results, MapShapeInspector reports the shape under
        // the wildcard "*" key, which the caller composes with the connection default.
        var hints = MapShapeInspector.InspectScalar(typeof(ClickHouseMap<string, int>));
        Assert.Equal(MapShape.Entries, hints);
    }

    [Fact]
    public void ScalarType_Dictionary_HintsDictionary()
    {
        var hints = MapShapeInspector.InspectScalar(typeof(Dictionary<string, int>));
        Assert.Equal(MapShape.Dictionary, hints);
    }

    [Fact]
    public void ScalarType_NonMap_HintsDefault()
    {
        Assert.Equal(MapShape.Default, MapShapeInspector.InspectScalar(typeof(int)));
        Assert.Equal(MapShape.Default, MapShapeInspector.InspectScalar(typeof(string)));
    }

    [Fact]
    public void Inspect_RepeatCalls_ReturnSameInstance()
    {
        // Lock in the per-Type caching: every QueryAsync<T> call goes through Inspect,
        // so dropping the cache would re-run reflection on every call. Reference
        // equality is the cheapest, most precise assertion.
        var first = MapShapeInspector.Inspect(typeof(ClickHouseMapPoco));
        var second = MapShapeInspector.Inspect(typeof(ClickHouseMapPoco));

        Assert.Same(first, second);
    }

    [Fact]
    public void Inspect_TypeWithNoMapProperties_ReturnsEmptySentinel()
    {
        // The Empty sentinel is what PushMapShapeHintFor checks with ReferenceEquals
        // to skip pushing a hint at all — must be reference-equal across calls.
        var a = MapShapeInspector.Inspect(typeof(NonMapPoco));
        var b = MapShapeInspector.Inspect(typeof(NonMapPoco));

        Assert.Same(a, b);
        Assert.Same(MapShapeInspector.Empty, a);
        Assert.Empty(a);
    }

    [Fact]
    public void InspectScalar_RepeatCalls_ReturnSameVerdict()
    {
        var first = MapShapeInspector.InspectScalar(typeof(ClickHouseMap<string, int>));
        var second = MapShapeInspector.InspectScalar(typeof(ClickHouseMap<string, int>));

        Assert.Equal(first, second);
        Assert.Equal(MapShape.Entries, first);
    }
}
