using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

public class GroupByTranslationTests
{
    public class Row
    {
        public int Id { get; set; }
        public string Country { get; set; } = "";
        public bool Active { get; set; }
        public decimal Amount { get; set; }
    }

    private static string GenerateSql(Func<IQueryable<Row>, IQueryable> transform)
    {
        var context = new ClickHouseQueryContext(null!, "row", typeof(Row));
        var queryable = new ClickHouseQueryable<Row>(context);
        var transformed = transform(queryable);
        return ((dynamic)transformed).ToSql();
    }

    [Fact]
    public void GroupBy_SingleKey_EmitsGroupByColumn()
    {
        var sql = GenerateSql(q => q.GroupBy(r => r.Country).Select(g => g.Key));

        Assert.Contains("GROUP BY", sql);
        Assert.Contains("`country`", sql);
    }

    [Fact]
    public void GroupBy_KeyAndCount_EmitsCountAggregate()
    {
        var sql = GenerateSql(q =>
            q.GroupBy(r => r.Country)
             .Select(g => new { g.Key, Count = g.Count() }));

        Assert.Contains("GROUP BY", sql);
        Assert.Contains("`country`", sql);
        Assert.Contains("count(", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void GroupBy_KeyAndSum_EmitsSumAggregate()
    {
        var sql = GenerateSql(q =>
            q.GroupBy(r => r.Country)
             .Select(g => new { g.Key, Total = g.Sum(r => r.Amount) }));

        Assert.Contains("GROUP BY", sql);
        Assert.Contains("`country`", sql);
        Assert.Contains("sum(`amount`)", sql);
    }

    [Fact]
    public void GroupBy_CompoundKey_EmitsBothColumns()
    {
        var sql = GenerateSql(q =>
            q.GroupBy(r => new { r.Country, r.Active })
             .Select(g => new { g.Key.Country, g.Key.Active, Count = g.Count() }));

        Assert.Contains("GROUP BY", sql);
        Assert.Contains("`country`", sql);
        Assert.Contains("`active`", sql);
    }

    [Fact]
    public void GroupBy_AfterWhere_PreservesFilter()
    {
        var sql = GenerateSql(q =>
            q.Where(r => r.Active)
             .GroupBy(r => r.Country)
             .Select(g => new { g.Key, Count = g.Count() }));

        Assert.Contains("WHERE", sql);
        Assert.Contains("`active`", sql);
        Assert.Contains("GROUP BY", sql);
        Assert.Contains("`country`", sql);
    }
}
