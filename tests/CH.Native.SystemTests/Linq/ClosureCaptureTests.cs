using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Linq.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Linq;

[Collection("LinqFacts")]
[Trait(Categories.Name, Categories.Linq)]
public class ClosureCaptureTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _node;
    private readonly LinqFactTableFixture _facts;
    private ClickHouseConnection _conn = null!;

    public ClosureCaptureTests(SingleNodeFixture node, LinqFactTableFixture facts)
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
    public async Task Closure_CapturedScalar_BecomesParameter()
    {
        // The markdown specifies that captured scalars should be PARAMETERISED
        // (no inline value in the SQL). The current implementation inlines them as
        // escaped literals (ClickHouseExpressionVisitor.cs constant emit). This
        // test pins the markdown's expected behaviour and is therefore EXPECTED
        // TO FAIL until the implementation switches to parameter binding.
        // See: /Users/db/.claude/plans/can-you-investigate-users-db-src-danielb-inherited-tarjan.md
        int min = 5;
        var query = _conn.Table<LinqFactRow>(_facts.TableName).Where(x => x.Id == min);
        string sql = query.ToSql();

        // Expected: the SQL contains a parameter placeholder, not the literal "5".
        Assert.DoesNotContain(" 5", sql);
        Assert.True(
            sql.Contains("?", StringComparison.Ordinal) ||
            sql.Contains("@", StringComparison.Ordinal) ||
            sql.Contains("{", StringComparison.Ordinal),
            $"Expected parameter placeholder in SQL, got: {sql}");

        // And it must still execute correctly.
        var rows = await query.ToListAsync();
        Assert.Single(rows);
        Assert.Equal(5, rows[0].Id);
    }

    [Fact]
    public async Task Closure_CapturedString_QuotedSafely()
    {
        // The fixture seeds a row with name = "O'Brien". A captured string
        // containing a single quote must round-trip without producing a SQL error.
        string needle = "O'Brien";

        var matches = await _conn.Table<LinqFactRow>(_facts.TableName)
            .Where(x => x.Name == needle)
            .Select(x => x.Name)
            .ToListAsync();

        Assert.Single(matches);
        Assert.Equal("O'Brien", matches[0]);
    }

    [Fact]
    public async Task Closure_CapturedList_TranslatesToInClause()
    {
        var ids = new long[] { 1, 2, 3 };

        var rows = await _conn.Table<LinqFactRow>(_facts.TableName)
            .Where(x => ids.Contains(x.Id))
            .OrderBy(x => x.Id)
            .Select(x => x.Id)
            .ToListAsync();

        Assert.Equal(ids, rows);
    }

    [Fact]
    public async Task Closure_EmptyList_InClause_ReturnsEmpty()
    {
        var ids = Array.Empty<long>();

        var rows = await _conn.Table<LinqFactRow>(_facts.TableName)
            .Where(x => ids.Contains(x.Id))
            .ToListAsync();

        Assert.Empty(rows);
    }
}
