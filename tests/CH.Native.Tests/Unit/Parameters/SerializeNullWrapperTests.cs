using CH.Native.Parameters;
using Xunit;

namespace CH.Native.Tests.Unit.Parameters;

/// <summary>
/// <see cref="ParameterSerializer"/>'s null decision (emit <c>NULL</c> vs throw) must account for the
/// transparent wrappers, so a null value typed <c>LowCardinality(Nullable(X))</c> /
/// <c>SimpleAggregateFunction(fn, Nullable(X))</c> is treated as nullable — previously the
/// prefix-only check threw for these.
/// </summary>
public class SerializeNullWrapperTests
{
    [Theory]
    [InlineData("Nullable(Int32)")]
    [InlineData("LowCardinality(Nullable(String))")]
    [InlineData("SimpleAggregateFunction(anyLast, Nullable(Float64))")]
    public void SerializeNull_EffectivelyNullable_EmitsEscapedNullMarker(string type) =>
        Assert.Equal("'\\\\N'", ParameterSerializer.Serialize(null, type));

    [Theory]
    [InlineData("String")]
    [InlineData("LowCardinality(String)")]
    [InlineData("SimpleAggregateFunction(anyLast, Float64)")]
    public void SerializeNull_NonNullable_Throws(string type) =>
        Assert.Throws<InvalidOperationException>(() => ParameterSerializer.Serialize(null, type));
}
