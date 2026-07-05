using CH.Native.Data.Types;
using Xunit;

namespace CH.Native.Tests.Unit.Data.Types;

/// <summary>
/// Covers <see cref="ClickHouseTypeParser.IsEffectivelyNullable"/> — the shared helper that fixes the
/// "nullability by string prefix misses transparent wrappers" bug class. A plain
/// <c>StartsWith("Nullable(")</c> misses the Nullable nested inside <c>LowCardinality(...)</c> and
/// <c>SimpleAggregateFunction(fn, T)</c>.
/// </summary>
public class EffectiveNullabilityTests
{
    [Theory]
    [InlineData("Nullable(Int32)", true)]
    [InlineData("Int32", false)]
    [InlineData("String", false)]
    [InlineData("LowCardinality(Nullable(String))", true)]
    [InlineData("LowCardinality(String)", false)]
    [InlineData("SimpleAggregateFunction(anyLast, Nullable(Float64))", true)]
    [InlineData("SimpleAggregateFunction(anyLast, Float64)", false)]
    [InlineData("SimpleAggregateFunction(groupArrayArray, Array(Int32))", false)]
    [InlineData("Array(Nullable(Int32))", false)]   // the array value itself is never NULL
    [InlineData("Map(String, Nullable(Int32))", false)]
    [InlineData("not a real type (", false)]        // unparseable → false, not a throw
    public void IsEffectivelyNullable_HandlesTransparentWrappers(string type, bool expected) =>
        Assert.Equal(expected, ClickHouseTypeParser.IsEffectivelyNullable(type));
}
