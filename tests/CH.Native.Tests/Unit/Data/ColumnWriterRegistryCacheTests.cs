using CH.Native.Data;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Pre-fix <see cref="ColumnWriterRegistry.GetWriter"/> allocated a new
/// <c>ColumnWriterFactory</c> for every composite-type lookup, mirroring
/// asymmetric behaviour with the cached <see cref="ColumnReaderRegistry"/>.
/// Bulk inserts that touch many composite types paid an allocation + factory
/// rebuild on every column. The fix caches composite writers via
/// <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey,TValue}.GetOrAdd"/>.
/// </summary>
public class ColumnWriterRegistryCacheTests
{
    [Theory]
    [InlineData("Nullable(Int32)")]
    [InlineData("Array(String)")]
    [InlineData("Map(String, Int32)")]
    [InlineData("Tuple(Int32, String)")]
    [InlineData("LowCardinality(String)")]
    [InlineData("FixedString(8)")]
    [InlineData("DateTime64(6)")]
    [InlineData("Decimal(10, 2)")]
    public void GetWriter_RepeatedCompositeLookup_ReturnsSameInstance(string typeName)
    {
        var registry = ColumnWriterRegistry.Default;
        var first = registry.GetWriter(typeName);
        var second = registry.GetWriter(typeName);
        Assert.Same(first, second);
    }

    [Fact]
    public void GetWriter_BuiltInScalar_StillResolves()
    {
        var registry = ColumnWriterRegistry.Default;
        var w = registry.GetWriter("Int32");
        Assert.NotNull(w);
        Assert.Equal("Int32", w.TypeName);
    }
}
