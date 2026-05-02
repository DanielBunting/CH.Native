using System.Linq.Expressions;
using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

public class AnyAllTranslationTests
{
    public class Row
    {
        public int Id { get; set; }
        public bool IsActive { get; set; }
    }

    private static string Translate(Expression e, ClickHouseQueryContext ctx) =>
        new ClickHouseExpressionVisitor(ctx).Translate(e);

    [Fact]
    public void Any_NoPredicate_EmitsSelectOneLimitOne()
    {
        var ctx = new ClickHouseQueryContext(null!, "row", typeof(Row));
        var q = new ClickHouseQueryable<Row>(ctx);
        var call = Expression.Call(typeof(Queryable), nameof(Queryable.Any),
            new[] { typeof(Row) }, q.Expression);

        var sql = Translate(call, ctx);

        Assert.Contains("SELECT 1", sql);
        Assert.Contains("LIMIT 1", sql);
    }

    [Fact]
    public void Any_WithPredicate_EmitsWhereSelectOneLimitOne()
    {
        var ctx = new ClickHouseQueryContext(null!, "row", typeof(Row));
        var q = new ClickHouseQueryable<Row>(ctx);
        Expression<Func<Row, bool>> pred = r => r.IsActive;
        var call = Expression.Call(typeof(Queryable), nameof(Queryable.Any),
            new[] { typeof(Row) }, q.Expression, pred);

        var sql = Translate(call, ctx);

        Assert.Contains("SELECT 1", sql);
        Assert.Contains("WHERE", sql);
        Assert.Contains("`is_active`", sql);
        Assert.Contains("LIMIT 1", sql);
    }

    [Fact]
    public void All_NegatesPredicateAndCounts()
    {
        // All(p) translates to: count() WHERE NOT p   (and the caller compares to 0)
        var ctx = new ClickHouseQueryContext(null!, "row", typeof(Row));
        var q = new ClickHouseQueryable<Row>(ctx);
        Expression<Func<Row, bool>> pred = r => r.IsActive;
        var call = Expression.Call(typeof(Queryable), nameof(Queryable.All),
            new[] { typeof(Row) }, q.Expression, pred);

        var sql = Translate(call, ctx);

        Assert.Contains("count(", sql);
        Assert.Contains("NOT", sql);
        Assert.Contains("`is_active`", sql);
    }
}
