using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Tests for the internal Map-shape resolution machinery that decides whether a given
/// column should be read as <see cref="Dictionary{TKey, TValue}"/> or as
/// <see cref="ClickHouseMap{TKey, TValue}"/>.
/// </summary>
public class MapShapeHintTests
{
    [Fact]
    public void Default_ResolvesToDictionary()
    {
        Assert.Equal(MapShape.Dictionary, MapShapeHint.Default.Resolve("any"));
    }

    [Fact]
    public void AllEntries_ResolvesToEntries_ForEveryColumn()
    {
        Assert.Equal(MapShape.Entries, MapShapeHint.AllEntries.Resolve("any"));
        Assert.Equal(MapShape.Entries, MapShapeHint.AllEntries.Resolve("other"));
    }

    [Fact]
    public void PerColumnEntries_BeatsDefaultFallback()
    {
        var perColumn = new Dictionary<string, MapShape>(StringComparer.Ordinal)
        {
            ["m"] = MapShape.Entries,
        };
        var hint = new MapShapeHint(perColumn);

        Assert.Equal(MapShape.Entries, hint.Resolve("m"));
        Assert.Equal(MapShape.Dictionary, hint.Resolve("other"));
    }

    [Fact]
    public void PerColumnDictionary_BeatsEntriesFallback()
    {
        var perColumn = new Dictionary<string, MapShape>(StringComparer.Ordinal)
        {
            ["m"] = MapShape.Dictionary,
        };
        var hint = new MapShapeHint(perColumn, MapShape.Entries);

        Assert.Equal(MapShape.Dictionary, hint.Resolve("m"));
        Assert.Equal(MapShape.Entries, hint.Resolve("other"));
    }

    [Fact]
    public void PerColumnDefault_FallsBackToFallback()
    {
        var perColumn = new Dictionary<string, MapShape>(StringComparer.Ordinal)
        {
            ["m"] = MapShape.Default,
        };
        var hint = new MapShapeHint(perColumn, MapShape.Entries);

        Assert.Equal(MapShape.Entries, hint.Resolve("m"));
    }

    [Fact]
    public void Fallback_DefaultSentinel_CollapsesToDictionary()
    {
        // The MapShape.Default sentinel is meaningless as a fallback — guard collapses
        // it to Dictionary so the factory never has to interpret Default.
        var hint = new MapShapeHint(perColumn: null, MapShape.Default);

        Assert.Equal(MapShape.Dictionary, hint.Fallback);
        Assert.Equal(MapShape.Dictionary, hint.Resolve("any"));
    }
}

/// <summary>
/// Tests proving the column reader factory selects between
/// <see cref="MapColumnReader{TKey, TValue}"/> and
/// <see cref="MapEntriesColumnReader{TKey, TValue}"/> based on the supplied
/// <see cref="MapShapeHint"/> and column name.
/// </summary>
public class ColumnReaderFactoryMapShapeTests
{
    [Fact]
    public void CreateReader_WithoutHint_ReturnsDictionaryReader()
    {
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default);

        var reader = factory.CreateReader("Map(String, Int32)");

        Assert.IsType<MapColumnReader<string, int>>(reader);
    }

    [Fact]
    public void CreateReader_AllEntriesHint_ReturnsEntriesReader()
    {
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default, MapShapeHint.AllEntries);

        var reader = factory.CreateReader("Map(String, Int32)", columnName: "m");

        Assert.IsType<MapEntriesColumnReader<string, int>>(reader);
    }

    [Fact]
    public void CreateReader_PerColumnEntries_ReturnsEntriesReader()
    {
        var perColumn = new Dictionary<string, MapShape>(StringComparer.Ordinal)
        {
            ["m"] = MapShape.Entries,
        };
        var hint = new MapShapeHint(perColumn);
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default, hint);

        var entriesReader = factory.CreateReader("Map(String, Int32)", columnName: "m");
        var dictReader = factory.CreateReader("Map(String, Int32)", columnName: "other");

        Assert.IsType<MapEntriesColumnReader<string, int>>(entriesReader);
        Assert.IsType<MapColumnReader<string, int>>(dictReader);
    }

    [Fact]
    public void CreateReader_PerColumnDictionary_BeatsEntriesFallback()
    {
        var perColumn = new Dictionary<string, MapShape>(StringComparer.Ordinal)
        {
            ["m"] = MapShape.Dictionary,
        };
        var hint = new MapShapeHint(perColumn, MapShape.Entries);
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default, hint);

        var reader = factory.CreateReader("Map(String, Int32)", columnName: "m");

        Assert.IsType<MapColumnReader<string, int>>(reader);
    }
}

/// <summary>
/// Tests for <c>ClickHouseConnection.PushMapShapeHintFor</c>, the typed-query entry
/// point that decides whether a per-call hint is needed at all and, if so, whether
/// it's the <see cref="MapShapeHint.AllEntries"/> shortcut (scalar <c>T</c>) or a
/// per-column hint derived from <c>T</c>'s properties.
/// </summary>
public class PushMapShapeHintForTests
{
    private static ClickHouseConnection NewUnopenedConnection() =>
        // Unit tests don't need to open the socket — PushMapShapeHintFor only
        // inspects the rowType. "Host=localhost" matches existing unit-test usage.
        new ClickHouseConnection("Host=localhost");

    [Fact]
    public async Task ScalarClickHouseMap_PushesAllEntriesHint()
    {
        await using var conn = NewUnopenedConnection();

        using (var _ = conn.PushMapShapeHintFor(typeof(ClickHouseMap<string, int>)))
        {
            var hint = conn.EffectiveMapShapeHintOrNull();
            Assert.NotNull(hint);
            Assert.Equal(MapShape.Entries, hint!.Resolve("anything"));
            Assert.Equal(MapShape.Entries, hint.Resolve("another"));
        }

        // After dispose, no hint is in effect.
        Assert.Null(conn.EffectiveMapShapeHintOrNull());
    }

    [Fact]
    public async Task PocoWithNoMapProperties_ReturnsNoOpDisposable()
    {
        await using var conn = NewUnopenedConnection();

        using (var disp = conn.PushMapShapeHintFor(typeof(NoMapPoco)))
        {
            // No hint pushed at all — the legacy Dictionary path stays in effect.
            Assert.Null(conn.EffectiveMapShapeHintOrNull());
            // Disposable is the cached no-op singleton (not strictly observable
            // from outside, but the null hint above proves the early-return path).
            Assert.NotNull(disp);
        }

        Assert.Null(conn.EffectiveMapShapeHintOrNull());
    }

    [Fact]
    public async Task PocoWithMixedMapProperties_PushesPerColumnHint()
    {
        await using var conn = NewUnopenedConnection();

        using (var _ = conn.PushMapShapeHintFor(typeof(MixedMapPoco)))
        {
            var hint = conn.EffectiveMapShapeHintOrNull();
            Assert.NotNull(hint);
            // ClickHouseMap property is entries; Dictionary property is dictionary;
            // non-Map property falls back to the default.
            Assert.Equal(MapShape.Entries, hint!.Resolve(nameof(MixedMapPoco.Tags)));
            Assert.Equal(MapShape.Dictionary, hint.Resolve(nameof(MixedMapPoco.Stats)));
            Assert.Equal(MapShape.Dictionary, hint.Resolve(nameof(MixedMapPoco.Name)));
        }

        Assert.Null(conn.EffectiveMapShapeHintOrNull());
    }

    private sealed class NoMapPoco
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class MixedMapPoco
    {
        public string Name { get; set; } = "";
        public Dictionary<string, int> Stats { get; set; } = new();
        public ClickHouseMap<string, int> Tags { get; set; } = null!;
    }
}
