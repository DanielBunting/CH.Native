using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

/// <summary>
/// LINQ <c>??</c> (Coalesce) is a BinaryExpression with NodeType=Coalesce.
/// Pre-fix the visitor's binary switch had no case for it, so the predicate
/// either threw NotSupportedException or fell through to a default arm that
/// emitted broken SQL. ClickHouse's null-coalesce is the <c>coalesce(a, b)</c>
/// scalar function; this test pins the translation.
/// </summary>
public class CoalesceTranslationTests
{
    public class Row
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    private static string GenerateSql(Func<IQueryable<Row>, IQueryable<Row>> transform)
    {
        var context = new ClickHouseQueryContext(null!, "row", typeof(Row));
        var queryable = new ClickHouseQueryable<Row>(context);
        return ((ClickHouseQueryable<Row>)transform(queryable)).ToSql();
    }

    [Fact]
    public void Coalesce_StringWithEmptyDefault_TranslatesToCoalesceFunction()
    {
        var sql = GenerateSql(q => q.Where(r => (r.Name ?? "") == "x"));

        Assert.Contains("coalesce(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("`name`", sql);
        Assert.Contains("''", sql);
    }

    [Fact]
    public void Coalesce_NestedInsideEquality_PreservesArgumentOrder()
    {
        var sql = GenerateSql(q => q.Where(r => (r.Name ?? "fallback") == "fallback"));

        var coalesceIdx = sql.IndexOf("coalesce(", StringComparison.OrdinalIgnoreCase);
        Assert.True(coalesceIdx >= 0, "expected coalesce(...) in SQL");

        var nameIdx = sql.IndexOf("`name`", coalesceIdx, StringComparison.Ordinal);
        var fallbackIdx = sql.IndexOf("'fallback'", coalesceIdx, StringComparison.Ordinal);
        Assert.True(nameIdx > 0 && fallbackIdx > 0, "both arms must appear after coalesce(");
        Assert.True(nameIdx < fallbackIdx, "left operand of ?? must come first inside coalesce()");
    }
}
