using CH.Native.Data;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

public class BlockSizeEstimatorTypeMatrixTests
{
    // Each fixed-size type at rowCount = 10.
    [Theory]
    [InlineData("Int8", 10)]
    [InlineData("UInt8", 10)]
    [InlineData("Bool", 10)]
    [InlineData("Int16", 20)]
    [InlineData("UInt16", 20)]
    [InlineData("Int32", 40)]
    [InlineData("UInt32", 40)]
    [InlineData("Float32", 40)]
    [InlineData("Date", 40)]
    [InlineData("IPv4", 40)]
    [InlineData("Int64", 80)]
    [InlineData("UInt64", 80)]
    [InlineData("Float64", 80)]
    [InlineData("DateTime", 80)]
    [InlineData("Date32", 80)]
    [InlineData("Int128", 160)]
    [InlineData("UInt128", 160)]
    [InlineData("UUID", 160)]
    [InlineData("IPv6", 160)]
    [InlineData("Int256", 320)]
    [InlineData("UInt256", 320)]
    [InlineData("Decimal32(4)", 40)]
    [InlineData("Decimal64(4)", 80)]
    [InlineData("Decimal128(4)", 160)]
    [InlineData("Decimal256(4)", 320)]
    [InlineData("DateTime64(3)", 80)]
    [InlineData("FixedString(7)", 70)]
    [InlineData("Enum8('a' = 1)", 10)]
    [InlineData("Enum16('a' = 1)", 20)]
    [InlineData("Nullable(Int32)", 50)]        // 10-byte null map + 40
    public void FixedTypes_EstimatedCorrectly(string type, long expected) =>
        Assert.Equal(expected, BlockSizeEstimator.EstimateMinimumSize(new[] { type }, 10));

    [Theory]
    [InlineData("String")]
    [InlineData("Array(Int32)")]
    [InlineData("Map(String, Int32)")]
    [InlineData("Tuple(Int32, String)")]
    [InlineData("LowCardinality(String)")]
    [InlineData("Nullable(String)")]           // variable inner => whole column variable
    [InlineData("FixedString(")]               // malformed: no closing paren -> length 0 (but still fixed 0)
    public void VariableOrDegenerate_ReturnMinusOneOrZero(string type)
    {
        var size = BlockSizeEstimator.EstimateMinimumSize(new[] { type }, 10);
        Assert.True(size == -1 || size == 0);
    }

    [Fact]
    public void FixedString_UnparseableLength_IsZero() =>
        Assert.Equal(0, BlockSizeEstimator.EstimateMinimumSize(new[] { "FixedString(abc)" }, 10));

    [Fact]
    public void ZeroRows_IsZero() =>
        Assert.Equal(0, BlockSizeEstimator.EstimateMinimumSize(new[] { "Int32", "String" }, 0));

    [Fact]
    public void MultipleColumns_SumFixed_ButMinusOneIfAnyVariable()
    {
        Assert.Equal(120, BlockSizeEstimator.EstimateMinimumSize(new[] { "Int32", "Int64" }, 10)); // 40 + 80
        Assert.Equal(-1, BlockSizeEstimator.EstimateMinimumSize(new[] { "Int32", "String" }, 10));
    }
}
