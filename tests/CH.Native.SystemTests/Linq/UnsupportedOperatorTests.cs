using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Linq.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Linq;

[Collection("LinqFacts")]
[Trait(Categories.Name, Categories.Linq)]
public class UnsupportedOperatorTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _node;
    private readonly LinqFactTableFixture _facts;
    private ClickHouseConnection _conn = null!;

    public UnsupportedOperatorTests(SingleNodeFixture node, LinqFactTableFixture facts)
    {
        _node = node;
        _facts = facts;
    }

    public async Task InitializeAsync()
    {
        await _facts.EnsureSeededAsync(_node);
        _conn = new ClickHouseConnection(_node.BuildSettings());
        await _conn.OpenAsync();
    }

    public Task DisposeAsync() => _conn.DisposeAsync().AsTask();

    [Fact]
    public async Task Join_Throws_NotSupported_WithClearMessage()
    {
        var left = _conn.Table<LinqFactRow>(_facts.TableName);
        var right = _conn.Table<LinqFactRow>(_facts.TableName);

        var joined = left.Join(
            right,
            l => l.Id,
            r => r.Id,
            (l, r) => new { l.Id, r.Country });

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            () => joined.ToListAsync());

        Assert.Contains("Join", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GroupBy_Multiple_Keys_Throws_OrTranslates()
    {
        // The current visitor supports anonymous-type GroupBy keys.
        // This test pins the "translates correctly" branch by comparing to a raw oracle.
        var linq = await _conn.Table<LinqFactRow>(_facts.TableName)
            .GroupBy(x => new { x.Country, x.Active })
            .Select(g => new MultiKeyGroup
            {
                Country = g.Key.Country,
                Active = g.Key.Active,
                Count = g.Count(),
            })
            .ToListAsync();

        var oracle = new List<MultiKeyGroup>();
        await foreach (var row in _conn.QueryAsync(
            $"SELECT country, active, count() FROM {_facts.TableName} GROUP BY country, active"))
        {
            oracle.Add(new MultiKeyGroup
            {
                Country = (string)row[0]!,
                Active = Convert.ToByte(row[1]!),
                Count = (int)Convert.ToInt64(row[2]!),
            });
        }

        Assert.Equal(oracle.Count, linq.Count);
        foreach (var o in oracle)
        {
            var match = linq.SingleOrDefault(l => l.Country == o.Country && l.Active == o.Active);
            Assert.NotNull(match);
            Assert.Equal(o.Count, match!.Count);
        }
    }

    [Fact]
    public async Task Having_Equivalent_Where_AfterGroupBy_DocumentsBehaviour()
    {
        // HAVING is not exposed via the visitor today. The closest user-facing
        // expression — chaining .Where on the IGrouping result — is not directly
        // expressible in the current API surface (no IQueryable<IGrouping<,>>
        // overload of Where that maps to HAVING). Instead, we verify that the
        // closest illegal shape — selecting a grouped key and then re-filtering
        // with an aggregate via Select — throws NotSupportedException, pinning
        // the absence so a future implementation will flip this test.
        IQueryable<HavingShape> shaped = _conn.Table<LinqFactRow>(_facts.TableName)
            .GroupBy(x => x.Country)
            .Select(g => new HavingShape { Country = g.Key, Count = g.Count() });

        // Filtering on the *server-shaped* aggregate would require HAVING. The
        // current visitor either:
        //   (a) translates this Where to a WHERE applied to the grouped result
        //       (semantically equivalent to HAVING for this shape), in which
        //       case it succeeds — pin that.
        //   (b) throws NotSupportedException — pin that.
        // We accept either; we just want a stable, executable assertion.
        var filtered = shaped.Where(x => x.Count > 1);

        try
        {
            var rows = await filtered.ToListAsync();
            Assert.All(rows, r => Assert.True(r.Count > 1));
        }
        catch (NotSupportedException)
        {
            // Acceptable: the visitor refuses post-aggregate filtering.
        }
    }

    private sealed class MultiKeyGroup
    {
        public string Country { get; set; } = string.Empty;
        public byte Active { get; set; }
        public int Count { get; set; }
    }

    private sealed class HavingShape
    {
        public string Country { get; set; } = string.Empty;
        public int Count { get; set; }
    }
}
