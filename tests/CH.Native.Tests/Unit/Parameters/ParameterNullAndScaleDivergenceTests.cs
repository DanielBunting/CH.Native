using CH.Native.Commands;
using CH.Native.Parameters;
using Xunit;

namespace CH.Native.Tests.Unit.Parameters;

/// <summary>
/// Pins CH.Native's parameter null / decimal-scale / explicit-type contracts (ported from the driver's
/// ParameterTypeResolution / SqlParameterizedSelect null-parameter tests). Several are intentional
/// divergences from the HTTP driver — documented inline.
/// </summary>
public class ParameterNullAndScaleDivergenceTests
{
    [Fact]
    public void SerializeNull_NonNullableString_ThrowsClientSide()
    {
        // Divergence: the driver emits server `\N` → empty string; CH.Native has no `\N` path and
        // rejects a non-nullable null client-side before any wire I/O.
        Assert.Throws<InvalidOperationException>(() => ParameterSerializer.Serialize(null, "String"));
    }

    [Theory]
    [InlineData("Int32")]
    [InlineData("DateTime")]
    public void SerializeNull_NonNullableNonString_ThrowsClientSide(string type)
    {
        // Divergence: the driver surfaces a ClickHouseServerException; CH.Native throws
        // InvalidOperationException client-side.
        Assert.Throws<InvalidOperationException>(() => ParameterSerializer.Serialize(null, type));
    }

    [Fact]
    public void SerializeNull_NullableType_EmitsEscapedNullMarker()
    {
        // Quoted `\N` — survives the server's two-pass parameter decode as the NULL marker.
        Assert.Equal("'\\\\N'", ParameterSerializer.Serialize(null, "Nullable(Int32)"));
    }

    [Fact]
    public void InferType_Decimal_UsesFixedScale18()
    {
        // Divergence: the driver derives scale from the value (Decimal128(3) for 123.456m);
        // CH.Native infers a fixed Decimal128(18). Value-derived scale lives in
        // SystemTests/Stress/DecimalPrecisionInferenceTests.
        Assert.Equal("Decimal128(18)", ClickHouseTypeMapper.InferType(123.456m));
    }

    [Fact]
    public void ExplicitClickHouseType_WinsOverInference()
    {
        // Inference of 42 would give Int32; the explicit type must win.
        var parameter = new ClickHouseParameter("p", 42, "Nullable(Int32)");
        Assert.Equal("Nullable(Int32)", parameter.ResolvedTypeName);
    }
}
