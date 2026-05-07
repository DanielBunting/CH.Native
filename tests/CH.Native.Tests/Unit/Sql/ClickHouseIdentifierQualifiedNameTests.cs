using CH.Native.Sql;
using Xunit;

namespace CH.Native.Tests.Unit.Sql;

public class ClickHouseIdentifierQualifiedNameTests
{
    [Theory]
    [InlineData("table", "`table`")]
    [InlineData("MyTable", "`MyTable`")]
    [InlineData("table with space", "`table with space`")]
    public void QuoteQualifiedName_BareNameQuotedLikeQuote(string raw, string expected)
        => Assert.Equal(expected, ClickHouseIdentifier.QuoteQualifiedName(raw));

    [Theory]
    [InlineData("db.table", "`db`.`table`")]
    [InlineData("db_a.events", "`db_a`.`events`")]
    [InlineData("MyDb.MyTable", "`MyDb`.`MyTable`")]
    public void QuoteQualifiedName_QualifiedNameSplitsOnSingleDot(string raw, string expected)
        => Assert.Equal(expected, ClickHouseIdentifier.QuoteQualifiedName(raw));

    [Fact]
    public void QuoteQualifiedName_EscapesEmbeddedBackticksPerSegment()
    {
        // `db`.`tab``le` — embedded backtick in the table segment is doubled inside its own quoting.
        Assert.Equal("`db`.`tab``le`", ClickHouseIdentifier.QuoteQualifiedName("db.tab`le"));
    }

    [Theory]
    [InlineData("a.b.c")]
    [InlineData("db.events.extra")]
    public void QuoteQualifiedName_MultipleDotsThrows(string raw)
        => Assert.Throws<ArgumentException>(() => ClickHouseIdentifier.QuoteQualifiedName(raw));

    [Theory]
    [InlineData(".table")]
    [InlineData("db.")]
    [InlineData("")]
    public void QuoteQualifiedName_EmptySegmentOrEmptyInputThrows(string raw)
        => Assert.Throws<ArgumentException>(() => ClickHouseIdentifier.QuoteQualifiedName(raw));

    [Fact]
    public void QuoteQualifiedName_NullArg_Throws()
        => Assert.Throws<ArgumentNullException>(() => ClickHouseIdentifier.QuoteQualifiedName(null!));
}
