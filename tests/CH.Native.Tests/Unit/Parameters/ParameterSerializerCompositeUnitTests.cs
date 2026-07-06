using CH.Native.Parameters;
using Xunit;

namespace CH.Native.Tests.Unit.Parameters;

/// <summary>
/// Covers the tuple/enum serialization arms of <see cref="ParameterSerializer"/> — including the
/// sub-branches that the integration round-trips don't reach: the element-type fallback when a tuple
/// has more elements than its declared type lists, a non-<c>Tuple(...)</c> declared type, and enum /
/// nested-tuple elements inside a composite literal.
/// </summary>
public class ParameterSerializerCompositeUnitTests
{
    private enum Color { Red = 1, Green = 2 }

    [Fact]
    public void Serialize_Enum_ByMemberName() =>
        Assert.Equal("'Red'", ParameterSerializer.Serialize(Color.Red, "Enum8('Red' = 1, 'Green' = 2)"));

    [Fact]
    public void Serialize_Tuple_ProducesLiteral() =>
        Assert.Equal("'(1, \\'a\\')'", ParameterSerializer.Serialize((1, "a"), "Tuple(Int32, String)"));

    [Fact]
    public void Serialize_Tuple_MoreElementsThanDeclaredTypes_FallsBackToString()
    {
        // 3 tuple elements but only one declared type -> elements 2 and 3 use the "String" fallback.
        var result = ParameterSerializer.Serialize((1, 2, 3), "Tuple(Int32)");
        Assert.Equal("'(1, 2, 3)'", result);
    }

    [Fact]
    public void Serialize_Tuple_NonTupleDeclaredType_TreatsAllElementsAsString()
    {
        // Declared type is not Tuple(...), so ExtractTupleElementTypes returns empty and every element
        // falls back to "String" — the value still serializes rather than throwing.
        var result = ParameterSerializer.Serialize((1, 2), "String");
        Assert.Equal("'(1, 2)'", result);
    }

    [Fact]
    public void Serialize_ArrayOfEnums_UsesMemberNames()
    {
        var result = ParameterSerializer.Serialize(new[] { Color.Red, Color.Green }, "Array(Enum8('Red' = 1, 'Green' = 2))");
        Assert.Contains("Red", result);
        Assert.Contains("Green", result);
    }

    [Fact]
    public void Serialize_NestedTuple_RecursesIntoInnerLiteral()
    {
        // Outer tuple whose first element is itself a tuple -> exercises the nested-tuple arm.
        var result = ParameterSerializer.Serialize(((1, 2), 3), "Tuple(Tuple(Int32, Int32), Int32)");
        Assert.Contains("(1, 2)", result);
        Assert.EndsWith("3)'", result);
    }
}
