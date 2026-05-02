using System.Linq.Expressions;
using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

/// <summary>
/// Sum is covered in <see cref="SqlGeneratorTests"/>; this pins Average / Min / Max,
/// which dispatch through the same VisitAggregate path but emit different functions.
/// </summary>
public class AggregateTranslationTests
{
    public class Row
    {
        public int Id { get; set; }
        public decimal Amount { get; set; }
        public int Quantity { get; set; }
    }

    private static string TranslateSingleTypeArg(string method, LambdaExpression selector)
    {
        var context = new ClickHouseQueryContext(null!, "row", typeof(Row));
        var queryable = new ClickHouseQueryable<Row>(context);
        var expression = Expression.Call(
            typeof(Queryable),
            method,
            new[] { typeof(Row) },
            queryable.Expression,
            selector);
        return new ClickHouseExpressionVisitor(context).Translate(expression);
    }

    // Min/Max have <TSource, TResult>, so two type args.
    private static string TranslateTwoTypeArgs(string method, Type resultType, LambdaExpression selector)
    {
        var context = new ClickHouseQueryContext(null!, "row", typeof(Row));
        var queryable = new ClickHouseQueryable<Row>(context);
        var expression = Expression.Call(
            typeof(Queryable),
            method,
            new[] { typeof(Row), resultType },
            queryable.Expression,
            selector);
        return new ClickHouseExpressionVisitor(context).Translate(expression);
    }

    [Fact]
    public void Average_OnDecimal_EmitsAvg()
    {
        Expression<Func<Row, decimal>> sel = r => r.Amount;
        var sql = TranslateSingleTypeArg(nameof(Queryable.Average), sel);
        Assert.Contains("avg(`amount`)", sql);
    }

    [Fact]
    public void Min_OnInt_EmitsMin()
    {
        Expression<Func<Row, int>> sel = r => r.Quantity;
        var sql = TranslateTwoTypeArgs(nameof(Queryable.Min), typeof(int), sel);
        Assert.Contains("min(`quantity`)", sql);
    }

    [Fact]
    public void Max_OnInt_EmitsMax()
    {
        Expression<Func<Row, int>> sel = r => r.Quantity;
        var sql = TranslateTwoTypeArgs(nameof(Queryable.Max), typeof(int), sel);
        Assert.Contains("max(`quantity`)", sql);
    }

    [Fact]
    public void Average_WithComputedExpression_TranslatesArithmetic()
    {
        Expression<Func<Row, int>> sel = r => r.Quantity * 2;
        var sql = TranslateSingleTypeArg(nameof(Queryable.Average), sel);
        Assert.Contains("avg(", sql);
        Assert.Contains("`quantity`", sql);
        Assert.Contains("*", sql);
    }

    [Fact]
    public void Sum_AfterWhere_BothClausesPresent()
    {
        var context = new ClickHouseQueryContext(null!, "row", typeof(Row));
        var queryable = new ClickHouseQueryable<Row>(context);

        Expression<Func<Row, bool>> pred = r => r.Quantity > 0;
        Expression<Func<Row, decimal>> sel = r => r.Amount;

        var whereCall = Expression.Call(
            typeof(Queryable), nameof(Queryable.Where),
            new[] { typeof(Row) },
            queryable.Expression, pred);

        var sumCall = Expression.Call(
            typeof(Queryable), nameof(Queryable.Sum),
            new[] { typeof(Row) },
            whereCall, sel);

        var sql = new ClickHouseExpressionVisitor(context).Translate(sumCall);

        Assert.Contains("sum(`amount`)", sql);
        Assert.Contains("WHERE", sql);
        Assert.Contains("`quantity`", sql);
    }
}
