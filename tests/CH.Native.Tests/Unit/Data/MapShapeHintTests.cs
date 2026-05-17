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
