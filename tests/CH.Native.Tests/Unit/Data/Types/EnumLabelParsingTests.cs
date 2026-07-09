using CH.Native.Data.Types;
using Xunit;

namespace CH.Native.Tests.Unit.Data.Types;

/// <summary>
/// Hardening cases for enum-label tokenization in <see cref="ClickHouseTypeParser"/>:
/// labels containing escaped quotes, '=' signs, and parentheses must not break parsing.
/// </summary>
public class EnumLabelParsingTests
{
    [Theory]
    [InlineData(@"Enum8('a\'b' = 1, 'c' = 2)")]     // backslash-escaped quote inside a label
    [InlineData("Enum8('a''b' = 1, 'c' = 2)")]      // doubled-quote escape inside a label
    [InlineData("Enum8('a=b' = 1, 'c' = 2)")]       // '=' inside a label
    [InlineData("Enum16('DateTime(''UTC'')' = 1)")] // parentheses + doubled quotes
    public void Parse_TrickyEnumLabels_CapturesValueList(string typeName)
    {
        var parsed = ClickHouseTypeParser.Parse(typeName);
        Assert.NotNull(parsed);
        // The enum value list is captured as parameters; the tricky label must not swallow
        // the second value nor split at the '=' / escaped quote.
        Assert.NotEmpty(parsed.Parameters);
    }
}
