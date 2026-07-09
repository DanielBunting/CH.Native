using CH.Native.Data;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// <see cref="BlockSizeEstimator"/> must size a <c>SimpleAggregateFunction(fn, T)</c> column as its
/// transparent inner type <c>T</c>, so a fixed inner type is not misreported as variable-length
/// (a perf regression). Mirrors the wrapper-unwrap applied on the write path.
/// </summary>
public class BlockSizeEstimatorWrapperTests
{
    [Fact]
    public void SimpleAggregateFunction_NullableInner_SizedAsInner()
    {
        var saf = BlockSizeEstimator.EstimateMinimumSize(
            new[] { "SimpleAggregateFunction(anyLast, Nullable(Float64))" }, rowCount: 10);
        var inner = BlockSizeEstimator.EstimateMinimumSize(new[] { "Nullable(Float64)" }, rowCount: 10);

        Assert.Equal(inner, saf);
        Assert.True(saf > 0); // fixed-size, not -1 (variable)
    }

    [Fact]
    public void SimpleAggregateFunction_FixedInner_SizedAsInner()
    {
        var saf = BlockSizeEstimator.EstimateMinimumSize(
            new[] { "SimpleAggregateFunction(anyLast, Float64)" }, rowCount: 10);
        var inner = BlockSizeEstimator.EstimateMinimumSize(new[] { "Float64" }, rowCount: 10);

        Assert.Equal(inner, saf);
    }
}
