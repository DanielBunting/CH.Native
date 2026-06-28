using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

/// <summary>
/// Regression tests for LINQ HAVING translation — release-prep item 1
/// (02-linq-preview.md). The builder always had <c>SqlBuilder.Having(string)</c>
/// (SqlBuilder.cs:107), but the expression visitor never dispatched to it, so a
/// post-group filter (<c>GroupBy(...).Where(g =&gt; aggregate)</c>) was silently
/// dropped — the predicate vanished and the query returned unfiltered rows. The fix
/// routes a <c>Where</c> whose predicate operates on an <c>IGrouping&lt;,&gt;</c> to
/// HAVING. These pin the <i>full emitted SQL</i> — clause ordering and the actual
/// predicate — not merely that the word "HAVING" appears, because a HAVING clause
/// that is present but mis-built (wrong predicate, wrong position) would still pass a
/// substring check while returning wrong rows.
/// </summary>
public class HavingTranslationTests
{
    public class Row
    {
        public int Id { get; set; }
        public string Country { get; set; } = "";
        public decimal Amount { get; set; }
    }

    private static string GenerateSql(Func<IQueryable<Row>, IQueryable> transform)
    {
        var context = new ClickHouseQueryContext(null!, "row", typeof(Row));
        var queryable = new ClickHouseQueryable<Row>(context);
        var transformed = transform(queryable);
        return ((dynamic)transformed).ToSql();
    }

    /// <summary>Asserts <paramref name="first"/> appears, and appears before <paramref name="second"/>.</summary>
    private static void AssertOrder(string sql, string first, string second)
    {
        var i = sql.IndexOf(first, StringComparison.Ordinal);
        var j = sql.IndexOf(second, StringComparison.Ordinal);
        Assert.True(i >= 0, $"Expected '{first}' in SQL: {sql}");
        Assert.True(j >= 0, $"Expected '{second}' in SQL: {sql}");
        Assert.True(i < j, $"Expected '{first}' before '{second}' in SQL: {sql}");
    }

    [Fact]
    public void GroupBy_FilterOnCountAfterGrouping_EmitsHavingAfterGroupBy()
    {
        var sql = GenerateSql(q =>
            q.GroupBy(r => r.Country)
             .Where(g => g.Count() > 5)
             .Select(g => new { g.Key, Count = g.Count() }));

        // GROUP BY must precede HAVING (HAVING references the grouped aggregate).
        AssertOrder(sql, "GROUP BY", "HAVING");
        // The predicate itself — a count comparison against the literal 5 — must land
        // in HAVING, not vanish. Anchor on the chars after HAVING so a stray "5"
        // elsewhere can't satisfy it.
        var having = sql[(sql.IndexOf("HAVING", StringComparison.Ordinal) + "HAVING".Length)..];
        Assert.Contains("count", having, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(">", having);
        Assert.Contains("5", having);
        // And it must NOT have been emitted as a pre-aggregation WHERE.
        Assert.DoesNotContain("WHERE", sql);
    }

    [Fact]
    public void GroupBy_FilterOnAggregateSum_EmitsHavingOnAggregate()
    {
        var sql = GenerateSql(q =>
            q.GroupBy(r => r.Country)
             .Where(g => g.Sum(r => r.Amount) > 1000m)
             .Select(g => new { g.Key, Total = g.Sum(r => r.Amount) }));

        AssertOrder(sql, "GROUP BY", "HAVING");
        var having = sql[(sql.IndexOf("HAVING", StringComparison.Ordinal) + "HAVING".Length)..];
        Assert.Contains("sum(`amount`)", having);
        Assert.Contains(">", having);
        Assert.Contains("1000", having);
    }

    [Fact]
    public void GroupBy_PreGroupWhereAndPostGroupHaving_EmitBothInOrder()
    {
        // A filter BEFORE GroupBy is a WHERE; a filter AFTER (on an aggregate) is a
        // HAVING. Both must appear, be distinct, and sit in the right positions:
        // WHERE before GROUP BY before HAVING.
        var sql = GenerateSql(q =>
            q.Where(r => r.Amount > 0m)
             .GroupBy(r => r.Country)
             .Where(g => g.Count() > 5)
             .Select(g => new { g.Key, Count = g.Count() }));

        AssertOrder(sql, "WHERE", "GROUP BY");
        AssertOrder(sql, "GROUP BY", "HAVING");

        // The pre-group predicate (amount > 0) belongs to WHERE, not HAVING.
        var whereToGroup = sql[(sql.IndexOf("WHERE", StringComparison.Ordinal))..sql.IndexOf("GROUP BY", StringComparison.Ordinal)];
        Assert.Contains("`amount`", whereToGroup);
        Assert.Contains("0", whereToGroup);
        Assert.DoesNotContain("count", whereToGroup, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void GroupBy_TwoPostGroupWhere_CombinedIntoSingleHavingWithAnd()
    {
        // Two aggregate filters after GroupBy must AND into one HAVING (SqlBuilder
        // ANDs successive Having calls) — not produce two HAVING clauses or drop one.
        var sql = GenerateSql(q =>
            q.GroupBy(r => r.Country)
             .Where(g => g.Count() > 5)
             .Where(g => g.Sum(r => r.Amount) > 1000m)
             .Select(g => new { g.Key, Count = g.Count() }));

        // Exactly one HAVING keyword.
        var first = sql.IndexOf("HAVING", StringComparison.Ordinal);
        Assert.True(first >= 0);
        Assert.Equal(-1, sql.IndexOf("HAVING", first + 1, StringComparison.Ordinal));

        var having = sql[(first + "HAVING".Length)..];
        Assert.Contains("AND", having);
        Assert.Contains("count", having, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("sum(`amount`)", having);
    }

    [Fact]
    public void GroupBy_HavingWithOrderBy_HavingBeforeOrderBy()
    {
        var sql = GenerateSql(q =>
            q.GroupBy(r => r.Country)
             .Where(g => g.Count() > 5)
             .OrderBy(g => g.Key)
             .Select(g => new { g.Key, Count = g.Count() }));

        AssertOrder(sql, "GROUP BY", "HAVING");
        AssertOrder(sql, "HAVING", "ORDER BY");
    }
}
