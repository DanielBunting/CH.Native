using System.Linq.Expressions;
using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

/// <summary>
/// The provider's contract: synchronous Execute must throw with a clear
/// "use async" message, CreateQuery must produce a typed ClickHouseQueryable
/// that exposes the same provider, and the typed/non-typed CreateQuery
/// overloads must agree on the underlying queryable.
/// </summary>
public class ClickHouseQueryProviderContractTests
{
    public class Row
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static (ClickHouseQueryProvider Provider, ClickHouseQueryable<Row> Queryable) MakeProvider()
    {
        var ctx = new ClickHouseQueryContext(null!, "row", typeof(Row));
        var queryable = new ClickHouseQueryable<Row>(ctx);
        return ((ClickHouseQueryProvider)queryable.Provider, queryable);
    }

    [Fact]
    public void Execute_Generic_Throws_NotSupportedWithAsyncHint()
    {
        var (provider, queryable) = MakeProvider();

        var ex = Assert.Throws<NotSupportedException>(() =>
            provider.Execute<List<Row>>(queryable.Expression));

        Assert.Contains("async", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ToListAsync", ex.Message);
    }

    [Fact]
    public void Execute_NonGeneric_Throws_NotSupportedWithAsyncHint()
    {
        var (provider, queryable) = MakeProvider();

        var ex = Assert.Throws<NotSupportedException>(() =>
            provider.Execute(queryable.Expression));

        Assert.Contains("async", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void CreateQuery_Generic_ReturnsClickHouseQueryable()
    {
        var (provider, queryable) = MakeProvider();

        var derived = provider.CreateQuery<Row>(queryable.Expression);

        Assert.IsType<ClickHouseQueryable<Row>>(derived);
        Assert.Same(provider, ((ClickHouseQueryable<Row>)derived).Provider);
    }

    [Fact]
    public void CreateQuery_NonGeneric_ReturnsTypedQueryable()
    {
        var (provider, queryable) = MakeProvider();

        var derived = provider.CreateQuery(queryable.Expression);

        Assert.IsType<ClickHouseQueryable<Row>>(derived);
    }

    [Fact]
    public void Provider_OnQueryable_IsClickHouseQueryProvider()
    {
        var (_, queryable) = MakeProvider();
        Assert.IsType<ClickHouseQueryProvider>(queryable.Provider);
    }

    [Fact]
    public void Queryable_ImplementsAsyncEnumerable_AndOrderedQueryable()
    {
        var (_, queryable) = MakeProvider();
        Assert.IsAssignableFrom<IAsyncEnumerable<Row>>(queryable);
        Assert.IsAssignableFrom<IOrderedQueryable<Row>>(queryable);
    }

    [Fact]
    public void GetEnumerator_Sync_Throws_NotSupported()
    {
        // The IEnumerable<T> path must reject sync iteration with the same
        // "use async" guidance — otherwise consumers who accidentally call
        // foreach over the queryable get an opaque NRE.
        var (_, queryable) = MakeProvider();

        Assert.Throws<NotSupportedException>(() =>
        {
            using var e = queryable.GetEnumerator();
            e.MoveNext();
        });
    }

    [Fact]
    public void Expression_RoundTripsThroughCreateQuery()
    {
        var (provider, queryable) = MakeProvider();

        // Build a Where call manually, hand it to CreateQuery, verify the
        // returned queryable's Expression is the one we passed in.
        Expression<Func<Row, bool>> pred = r => r.Id > 0;
        var whereCall = Expression.Call(
            typeof(Queryable), nameof(Queryable.Where),
            new[] { typeof(Row) },
            queryable.Expression, pred);

        var derived = (IQueryable<Row>)provider.CreateQuery<Row>(whereCall);
        Assert.Same(whereCall, derived.Expression);
    }
}
