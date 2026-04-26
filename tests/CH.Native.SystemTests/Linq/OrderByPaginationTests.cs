using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Linq.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Linq;

[Collection("LinqFacts")]
[Trait(Categories.Name, Categories.Linq)]
public class OrderByPaginationTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _node;
    private readonly LinqFactTableFixture _facts;
    private ClickHouseConnection _conn = null!;

    public OrderByPaginationTests(SingleNodeFixture node, LinqFactTableFixture facts)
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
    public async Task OrderBy_Skip_Take_RespectsOrder()
    {
        var page = await _conn.Table<LinqFactRow>(_facts.TableName)
            .OrderBy(x => x.Id)
            .Skip(10)
            .Take(5)
            .Select(x => x.Id)
            .ToListAsync();

        Assert.Equal(new long[] { 11, 12, 13, 14, 15 }, page);
    }

    [Fact]
    public async Task OrderBy_NoStableSort_DocumentsBehaviour()
    {
        // Order by a non-unique column (Active is 0/1) and Take(1).
        // ClickHouse does not guarantee a stable sort across non-unique keys.
        // We assert the call succeeds and the row is one of the matching rows.
        var picked = await _conn.Table<LinqFactRow>(_facts.TableName)
            .OrderBy(x => x.Active)
            .Take(1)
            .Select(x => x.Active)
            .ToListAsync();

        Assert.Single(picked);
        Assert.Equal((byte)0, picked[0]); // 0 sorts before 1
    }

    [Fact]
    public async Task Take_Without_OrderBy_NoFlakiness_OnSmallTable()
    {
        var rows = await _conn.Table<LinqFactRow>(_facts.TableName)
            .Take(5)
            .ToListAsync();

        Assert.Equal(5, rows.Count);
    }

    [Fact]
    public async Task OrderByDescending_ThenBy_MultipleColumns()
    {
        var linqOrder = await _conn.Table<LinqFactRow>(_facts.TableName)
            .OrderByDescending(x => x.Active)
            .ThenBy(x => x.Id)
            .Select(x => new IdActive { Id = x.Id, Active = x.Active })
            .ToListAsync();

        var oracle = new List<IdActive>();
        await foreach (var row in _conn.QueryAsync(
            $"SELECT id, active FROM {_facts.TableName} ORDER BY active DESC, id ASC"))
        {
            oracle.Add(new IdActive
            {
                Id = Convert.ToInt64(row[0]!),
                Active = Convert.ToByte(row[1]!),
            });
        }

        Assert.Equal(oracle.Count, linqOrder.Count);
        for (int i = 0; i < oracle.Count; i++)
        {
            Assert.Equal(oracle[i].Id, linqOrder[i].Id);
            Assert.Equal(oracle[i].Active, linqOrder[i].Active);
        }
    }

    private sealed class IdActive
    {
        public long Id { get; set; }
        public byte Active { get; set; }
    }
}
