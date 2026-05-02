using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

/// <summary>
/// C# ternary <c>a ? b : c</c> arrives as a ConditionalExpression. The visitor
/// translates it to ClickHouse's <c>if(a, b, c)</c> scalar function. Pin both
/// the function name and the argument order — flipping IfTrue/IfFalse silently
/// would be a high-impact bug.
/// </summary>
public class ConditionalTranslationTests
{
    public class Row
    {
        public int A { get; set; }
        public int B { get; set; }
        public bool IsActive { get; set; }
    }

    private static string GenerateSql(Func<IQueryable<Row>, IQueryable<Row>> transform)
    {
        var context = new ClickHouseQueryContext(null!, "row", typeof(Row));
        var queryable = new ClickHouseQueryable<Row>(context);
        return ((ClickHouseQueryable<Row>)transform(queryable)).ToSql();
    }

    [Fact]
    public void Ternary_TranslatesToIfFunction()
    {
        var sql = GenerateSql(q => q.Where(r => (r.IsActive ? r.A : r.B) > 0));

        Assert.Contains("if(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("`is_active`", sql);
        Assert.Contains("`a`", sql);
        Assert.Contains("`b`", sql);
    }

    [Fact]
    public void Ternary_PreservesArgumentOrder()
    {
        var sql = GenerateSql(q => q.Where(r => (r.IsActive ? r.A : r.B) > 0));

        var ifIdx = sql.IndexOf("if(", StringComparison.OrdinalIgnoreCase);
        Assert.True(ifIdx >= 0);

        var aIdx = sql.IndexOf("`a`", ifIdx, StringComparison.Ordinal);
        var bIdx = sql.IndexOf("`b`", ifIdx, StringComparison.Ordinal);
        Assert.True(aIdx > 0 && bIdx > aIdx,
            "IfTrue branch (`a`) must come before IfFalse branch (`b`) in if(condition, ifTrue, ifFalse)");
    }
}
