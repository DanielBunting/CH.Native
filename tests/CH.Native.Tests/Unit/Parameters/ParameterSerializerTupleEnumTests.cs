using CH.Native.Parameters;
using Xunit;

namespace CH.Native.Tests.Unit.Parameters;

/// <summary>
/// Covers Tuple/ValueTuple and .NET enum serialization added to <see cref="ParameterSerializer"/>.
/// </summary>
public class ParameterSerializerTupleEnumTests
{
    private enum Color
    {
        Red,
        Green,
        Blue,
    }

    [Fact]
    public void Serialize_NumericTuple_ProducesTupleLiteral() =>
        // Digits/parens/commas are not altered by the outer parameter-escape wrapper.
        Assert.Equal("'(1, 2)'", ParameterSerializer.Serialize((1, 2), "Tuple(Int32, Int32)"));

    [Fact]
    public void Serialize_MixedTuple_QuotesStringElement()
    {
        var result = ParameterSerializer.Serialize((1, "a"), "Tuple(Int32, String)");
        // Raw literal is (1, 'a'); the inner quotes are escaped by the parameter wrapper.
        Assert.StartsWith("'(1, ", result);
        Assert.Contains("a", result);
    }

    [Fact]
    public void Serialize_Enum_UsesMemberNameAsLabel() =>
        Assert.Equal("'Green'", ParameterSerializer.Serialize(Color.Green, "Enum8('Red'=0,'Green'=1,'Blue'=2)"));

    [Fact]
    public void Serialize_TupleContainingEnum_Roundtrips()
    {
        var result = ParameterSerializer.Serialize((1, Color.Blue), "Tuple(Int32, Enum8('Red'=0,'Green'=1,'Blue'=2))");
        Assert.StartsWith("'(1, ", result);
        Assert.Contains("Blue", result);
    }
}
