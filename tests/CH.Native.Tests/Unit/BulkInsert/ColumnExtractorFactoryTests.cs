using System.Reflection;
using CH.Native.BulkInsert;
using CH.Native.Mapping;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

public class ColumnExtractorFactoryTests
{
    #region Unsupported composite types throw NotSupportedException

    [Fact]
    public void Create_IntArrayProperty_ThrowsNotSupported()
    {
        var property = typeof(ArrayRow).GetProperty(nameof(ArrayRow.Values))!;

        Assert.Throws<NotSupportedException>(() =>
            ColumnExtractorFactory.Create<ArrayRow>(property, "values", "Array(Int32)"));
    }

    [Fact]
    public void Create_DictionaryProperty_ThrowsNotSupported()
    {
        var property = typeof(MapRow).GetProperty(nameof(MapRow.Metadata))!;

        Assert.Throws<NotSupportedException>(() =>
            ColumnExtractorFactory.Create<MapRow>(property, "metadata", "Map(String, Int32)"));
    }

    [Fact]
    public void Create_ObjectArrayProperty_ThrowsNotSupported()
    {
        var property = typeof(TupleRow).GetProperty(nameof(TupleRow.Pair))!;

        Assert.Throws<NotSupportedException>(() =>
            ColumnExtractorFactory.Create<TupleRow>(property, "pair", "Tuple(Int32, String)"));
    }

    #endregion

    #region Non-composite types return typed extractors

    [Fact]
    public void Create_IntProperty_ReturnsTypedExtractor()
    {
        var property = typeof(SimpleRow).GetProperty(nameof(SimpleRow.Id))!;

        var extractor = ColumnExtractorFactory.Create<SimpleRow>(property, "id", "Int32");

        Assert.NotNull(extractor);
    }

    [Fact]
    public void Create_StringProperty_ReturnsTypedExtractor()
    {
        var property = typeof(SimpleRow).GetProperty(nameof(SimpleRow.Name))!;

        var extractor = ColumnExtractorFactory.Create<SimpleRow>(property, "name", "String");

        Assert.NotNull(extractor);
    }

    #endregion

    #region Test POCOs

    private class ArrayRow
    {
        public int[] Values { get; set; } = Array.Empty<int>();
    }

    private class MapRow
    {
        public Dictionary<string, int> Metadata { get; set; } = new();
    }

    private class TupleRow
    {
        public object?[] Pair { get; set; } = Array.Empty<object?>();
    }

    private class SimpleRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    #endregion
}
