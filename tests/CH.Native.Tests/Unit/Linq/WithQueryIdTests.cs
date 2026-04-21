using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

public class WithQueryIdTests
{
    private sealed class Row
    {
        public int Id { get; set; }
    }

    [Fact]
    public void WithQueryId_SetsContextQueryId()
    {
        var context = new ClickHouseQueryContext(
            connection: null!,
            tableName: "row",
            elementType: typeof(Row));

        var queryable = new ClickHouseQueryable<Row>(context).AsQueryable();
        queryable.WithQueryId("my-linq-id");

        Assert.Equal("my-linq-id", context.QueryId);
    }

    [Fact]
    public void WithQueryId_ThrowsOnNonClickHouseSource()
    {
        var plain = new[] { new Row() }.AsQueryable();
        Assert.Throws<InvalidOperationException>(() => plain.WithQueryId("x"));
    }
}
