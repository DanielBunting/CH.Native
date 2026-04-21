using CH.Native.Sql;
using Xunit;

namespace CH.Native.Tests.Unit.Sql;

public class ClickHouseIdentifierTests
{
    [Theory]
    [InlineData("role_a",         "`role_a`")]
    [InlineData("",               "``")]
    [InlineData("role with space","`role with space`")]
    [InlineData("unicode_角色",    "`unicode_角色`")]
    public void Quote_LeavesInnocuousIdentifiersIntact(string raw, string expected)
        => Assert.Equal(expected, ClickHouseIdentifier.Quote(raw));

    [Theory]
    [InlineData("r`x",    "`r``x`")]
    [InlineData("`",      "````")]       // single backtick → open ` + escaped `` + close ` = 4
    [InlineData("``",     "``````")]     // two backticks → open ` + escaped ```` + close ` = 6
    [InlineData("a`b`c",  "`a``b``c`")]
    public void Quote_DoublesEmbeddedBackticks(string raw, string expected)
        => Assert.Equal(expected, ClickHouseIdentifier.Quote(raw));

    [Theory]
    // Common SQL-injection patterns in role names must be rendered inert.
    [InlineData("a`; DROP TABLE x; --",   "`a``; DROP TABLE x; --`")]
    [InlineData("a` OR 1=1 --",           "`a`` OR 1=1 --`")]
    [InlineData("'; SELECT 1 --",         "`'; SELECT 1 --`")]
    public void Quote_NeutralisesInjectionAttempts(string raw, string expected)
        => Assert.Equal(expected, ClickHouseIdentifier.Quote(raw));

    [Fact]
    public void Quote_NullArg_Throws()
        => Assert.Throws<ArgumentNullException>(() => ClickHouseIdentifier.Quote(null!));
}
