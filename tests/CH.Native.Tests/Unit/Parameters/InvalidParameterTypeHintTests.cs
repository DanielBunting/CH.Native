using CH.Native.Commands;
using CH.Native.Data.Types;
using Xunit;

namespace CH.Native.Tests.Unit.Parameters;

/// <summary>
/// Covers the client-side syntax gate for parameter type hints. A syntactically malformed hint is
/// rejected as <see cref="ArgumentException"/> at the parameter boundary; a well-formed but unknown
/// type name (e.g. <c>NotAType</c>) is accepted client-side (it is a valid identifier) and left for
/// the server to reject — a documented divergence from the HTTP driver, which rejects it eagerly.
/// </summary>
public class InvalidParameterTypeHintTests
{
    [Theory]
    [InlineData("Int(")]
    [InlineData("Array(")]
    [InlineData("Tuple(Int32,")]
    [InlineData("Int32;DROP")]
    public void MalformedTypeHint_OnParameter_ThrowsArgumentException(string type) =>
        Assert.Throws<ArgumentException>(() => new ClickHouseParameter { ClickHouseType = type });

    [Theory]
    [InlineData("Int(")]
    [InlineData("Array(")]
    public void MalformedTypeHint_OnParser_ThrowsFormatException(string type) =>
        Assert.Throws<FormatException>(() => ClickHouseTypeParser.Parse(type));

    [Fact]
    public void UnknownButWellFormedTypeName_IsAcceptedClientSide()
    {
        // "NotAType" is a valid identifier, so the client-side syntax gate accepts it; the server
        // is the component that rejects the unknown type at query time.
        var parameter = new ClickHouseParameter { ClickHouseType = "NotAType" };
        Assert.Equal("NotAType", parameter.ClickHouseType);
    }
}
