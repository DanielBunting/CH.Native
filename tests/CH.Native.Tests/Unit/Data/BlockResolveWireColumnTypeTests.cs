using CH.Native.Data;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Covers <see cref="Block.ResolveWireColumnType"/> — the block-header type resolution that unwraps a
/// top-level <c>SimpleAggregateFunction(fn, T)</c> to its inner <c>T</c> and passes everything else
/// through unchanged (including the defensive fallback for a malformed SAF string).
/// </summary>
public class BlockResolveWireColumnTypeTests
{
    [Theory]
    [InlineData("Int32", "Int32")]
    [InlineData("Nullable(Float64)", "Nullable(Float64)")]
    [InlineData("Array(String)", "Array(String)")]
    [InlineData("LowCardinality(String)", "LowCardinality(String)")]   // not a SAF — passes through
    public void NonSimpleAggregateFunction_PassesThrough(string input, string expected) =>
        Assert.Equal(expected, Block.ResolveWireColumnType(input));

    [Theory]
    [InlineData("SimpleAggregateFunction(anyLast, Nullable(Float64))", "Nullable(Float64)")]
    [InlineData("SimpleAggregateFunction(sum, Int64)", "Int64")]
    public void SimpleAggregateFunction_UnwrapsToInnerType(string input, string expected) =>
        Assert.Equal(expected, Block.ResolveWireColumnType(input));

    [Fact]
    public void MalformedSimpleAggregateFunction_FallsBackToInput()
    {
        // Starts with the SAF prefix but does not parse to a single-argument SAF — the defensive
        // fallback returns the original string rather than throwing.
        const string malformed = "SimpleAggregateFunction(";
        Assert.Equal(malformed, Block.ResolveWireColumnType(malformed));
    }
}
