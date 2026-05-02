using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

/// <summary>
/// Validation for negative / extreme arguments to Take and Skip. Pre-fix the
/// visitor passed the int through verbatim, emitting <c>LIMIT -1</c> (which
/// ClickHouse parses inconsistently across versions) and accepting negative
/// <c>OFFSET</c> silently. The contract aligns with <c>Enumerable.Take</c>
/// (negative ⇒ empty result) and <c>Enumerable.Skip</c> (negative ⇒ throws).
/// </summary>
public class TakeSkipValidationTests
{
    public class Row
    {
        public int Id { get; set; }
    }

    private static string GenerateSql(Func<IQueryable<Row>, IQueryable<Row>> transform)
    {
        var context = new ClickHouseQueryContext(null!, "row", typeof(Row));
        var queryable = new ClickHouseQueryable<Row>(context);
        return ((ClickHouseQueryable<Row>)transform(queryable)).ToSql();
    }

    [Fact]
    public void Take_NegativeCount_ProducesLimitZero()
    {
        var sql = GenerateSql(q => q.Take(-3));

        Assert.Contains("LIMIT 0", sql);
        Assert.DoesNotContain("LIMIT -", sql);
    }

    [Fact]
    public void Take_Zero_ProducesLimitZero()
    {
        var sql = GenerateSql(q => q.Take(0));

        Assert.Contains("LIMIT 0", sql);
    }

    [Fact]
    public void Take_PositiveCount_ProducesLimitN()
    {
        var sql = GenerateSql(q => q.Take(7));

        Assert.Contains("LIMIT 7", sql);
    }

    [Fact]
    public void Skip_NegativeCount_ThrowsArgumentOutOfRange()
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => GenerateSql(q => q.Skip(-5)));
        Assert.Contains("non-negative", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Skip_Zero_ProducesNoOffsetOrZeroOffset()
    {
        var sql = GenerateSql(q => q.Skip(0));

        Assert.DoesNotContain("OFFSET -", sql);
    }
}
