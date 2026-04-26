using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

/// <summary>
/// Regression tests for the LIKE-pattern escaping in
/// <c>ClickHouseExpressionVisitor.EscapeLikePattern</c>. The bug: only <c>'</c>
/// and <c>\</c> were escaped, while <c>%</c> and <c>_</c> (SQL wildcards) were
/// not — so <c>name.Contains("50%")</c> matched "501", "502", etc. rather than
/// the intended literal <c>50%</c>. Additionally, escaping must happen on the
/// user's value BEFORE the wildcard wrapper is applied, otherwise the wrapping
/// <c>%</c> characters themselves would be escaped.
/// </summary>
public class LikeEscapingTests
{
    private class Row
    {
        public string Name { get; set; } = "";
    }

    private static string GenerateSql(Func<IQueryable<Row>, IQueryable<Row>> transform)
    {
        var context = new ClickHouseQueryContext(null!, "row", typeof(Row));
        var queryable = new ClickHouseQueryable<Row>(context);
        return ((ClickHouseQueryable<Row>)transform(queryable)).ToSql();
    }

    // ---- Literal % must be escaped (Contains) ----------------------------

    [Fact]
    public void Contains_LiteralPercent_IsEscaped()
    {
        var sql = GenerateSql(q => q.Where(r => r.Name.Contains("50%")));

        // Wrapping wildcards intact; literal % in the search value escaped.
        Assert.Contains(@"LIKE '%50\%%'", sql);
    }

    [Fact]
    public void Contains_LiteralUnderscore_IsEscaped()
    {
        var sql = GenerateSql(q => q.Where(r => r.Name.Contains("user_")));

        Assert.Contains(@"LIKE '%user\_%'", sql);
    }

    [Fact]
    public void Contains_LiteralBackslash_IsEscaped()
    {
        var sql = GenerateSql(q => q.Where(r => r.Name.Contains(@"a\b")));

        // User's backslash must end up as \\ in the SQL literal (ClickHouse
        // parses \\ as a single backslash, which LIKE then treats as literal).
        Assert.Contains(@"LIKE '%a\\b%'", sql);
    }

    // ---- Same three checks for StartsWith --------------------------------

    [Fact]
    public void StartsWith_LiteralPercent_IsEscaped()
    {
        var sql = GenerateSql(q => q.Where(r => r.Name.StartsWith("50%")));

        Assert.Contains(@"LIKE '50\%%'", sql);
    }

    [Fact]
    public void StartsWith_LiteralUnderscore_IsEscaped()
    {
        var sql = GenerateSql(q => q.Where(r => r.Name.StartsWith("user_")));

        Assert.Contains(@"LIKE 'user\_%'", sql);
    }

    // ---- Same for EndsWith -----------------------------------------------

    [Fact]
    public void EndsWith_LiteralPercent_IsEscaped()
    {
        var sql = GenerateSql(q => q.Where(r => r.Name.EndsWith("%done")));

        Assert.Contains(@"LIKE '%\%done'", sql);
    }

    [Fact]
    public void EndsWith_LiteralUnderscore_IsEscaped()
    {
        var sql = GenerateSql(q => q.Where(r => r.Name.EndsWith("_v1")));

        Assert.Contains(@"LIKE '%\_v1'", sql);
    }

    // ---- Baseline: plain values still work (no regression) ---------------

    [Fact]
    public void Contains_PlainValue_WrappedWithWildcards()
    {
        var sql = GenerateSql(q => q.Where(r => r.Name.Contains("Smith")));
        Assert.Contains("LIKE '%Smith%'", sql);
    }

    [Fact]
    public void StartsWith_PlainValue_TrailingWildcardOnly()
    {
        var sql = GenerateSql(q => q.Where(r => r.Name.StartsWith("Smith")));
        Assert.Contains("LIKE 'Smith%'", sql);
    }

    [Fact]
    public void EndsWith_PlainValue_LeadingWildcardOnly()
    {
        var sql = GenerateSql(q => q.Where(r => r.Name.EndsWith("Smith")));
        Assert.Contains("LIKE '%Smith'", sql);
    }

    // ---- Quote escaping still works --------------------------------------

    [Fact]
    public void Contains_SingleQuote_IsEscaped()
    {
        var sql = GenerateSql(q => q.Where(r => r.Name.Contains("O'Brien")));

        Assert.Contains(@"LIKE '%O\'Brien%'", sql);
    }
}
