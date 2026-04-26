using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Linq.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Linq;

[Collection("LinqFacts")]
[Trait(Categories.Name, Categories.Linq)]
public class DistinctAnyAllTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _node;
    private readonly LinqFactTableFixture _facts;
    private ClickHouseConnection _conn = null!;

    public DistinctAnyAllTests(SingleNodeFixture node, LinqFactTableFixture facts)
    {
        _node = node;
        _facts = facts;
    }

    public async Task InitializeAsync()
    {
        _conn = new ClickHouseConnection(_node.BuildSettings());
        await _conn.OpenAsync();
    }

    public Task DisposeAsync() => _conn.DisposeAsync().AsTask();

    [Fact]
    public async Task Distinct_OnString_DeduplicatesCaseSensitive()
    {
        var distinct = await _conn.Table<LinqFactRow>(_facts.TableName)
            .Select(x => x.Country)
            .Distinct()
            .ToListAsync();

        // Fixture uses 5 distinct ASCII countries.
        var expected = _facts.Rows.Select(r => r.Country).Distinct().OrderBy(c => c).ToList();
        var actual = distinct.OrderBy(c => c).ToList();

        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Any_WithPredicate_ShortCircuitsOnServer()
    {
        // Fixture seeds amounts in [-200, 1000] so some are negative.
        bool any = await _conn.Table<LinqFactRow>(_facts.TableName)
            .AnyAsync(x => x.Amount < 0);

        bool oracle = _facts.Rows.Any(r => r.Amount < 0);
        Assert.Equal(oracle, any);
    }

    [Fact]
    public async Task All_WithPredicate_TrueOnEmpty()
    {
        // Vacuous truth: All over an empty set is true.
        bool all = await _conn.Table<LinqFactRow>(_facts.TableName)
            .Where(x => x.Id < 0)
            .AllAsync(x => x.Amount > 0);

        Assert.True(all);
    }
}
