using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

/// <summary>
/// VisitBinaryPredicate handles the arithmetic operators + - * / %. Pin the
/// SQL operator each maps to so the visitor's switch can't drift silently.
/// </summary>
public class BinaryArithmeticTranslationTests
{
    public class Row
    {
        public int A { get; set; }
        public int B { get; set; }
    }

    private static string GenerateSql(Func<IQueryable<Row>, IQueryable<Row>> transform)
    {
        var context = new ClickHouseQueryContext(null!, "row", typeof(Row));
        var queryable = new ClickHouseQueryable<Row>(context);
        return ((ClickHouseQueryable<Row>)transform(queryable)).ToSql();
    }

    [Fact]
    public void Add_TranslatesToPlus()
    {
        var sql = GenerateSql(q => q.Where(r => r.A + r.B == 10));
        Assert.Contains("`a`", sql);
        Assert.Contains("+", sql);
        Assert.Contains("`b`", sql);
    }

    [Fact]
    public void Subtract_TranslatesToMinus()
    {
        var sql = GenerateSql(q => q.Where(r => r.A - r.B == 0));
        Assert.Contains("-", sql);
    }

    [Fact]
    public void Multiply_TranslatesToStar()
    {
        var sql = GenerateSql(q => q.Where(r => r.A * 2 > 100));
        Assert.Contains("*", sql);
    }

    [Fact]
    public void Divide_TranslatesToSlash()
    {
        var sql = GenerateSql(q => q.Where(r => r.A / 2 > 5));
        Assert.Contains("/", sql);
    }

    [Fact]
    public void Modulo_TranslatesToPercent()
    {
        var sql = GenerateSql(q => q.Where(r => r.A % 2 == 0));
        Assert.Contains("%", sql);
    }

    [Fact]
    public void Negate_EmitsLeadingMinus()
    {
        var sql = GenerateSql(q => q.Where(r => -r.A > 10));
        Assert.Contains("-(", sql);
    }
}
