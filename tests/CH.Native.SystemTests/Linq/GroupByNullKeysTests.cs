using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Linq;

/// <summary>
/// Pins the LINQ GroupBy contract for keys that contain NULLs. CLR LINQ-to-objects
/// groups all null keys into a single group; ClickHouse SQL semantics also
/// group nulls together. This test verifies the LINQ-to-ClickHouse path
/// produces the same observed result as raw SQL — preventing a translation
/// regression that would split null keys into one-row-per-null groups (a
/// previously-common CH bug pattern).
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Linq)]
public class GroupByNullKeysTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;
    private string _table = null!;
    private ClickHouseConnection _conn = null!;

    public GroupByNullKeysTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    public async Task InitializeAsync()
    {
        _table = $"groupby_null_{Guid.NewGuid():N}";
        _conn = new ClickHouseConnection(_fx.BuildSettings());
        await _conn.OpenAsync();
        await _conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {_table} (id Int32, category Nullable(String), amount Int32) ENGINE = Memory");
        await _conn.ExecuteNonQueryAsync(
            $"INSERT INTO {_table} VALUES " +
            "(1, 'A', 10), (2, 'A', 20), (3, NULL, 30), (4, NULL, 40), (5, 'B', 50), (6, NULL, 60)");
    }

    public async Task DisposeAsync()
    {
        try { await _conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_table}"); } catch { }
        await _conn.DisposeAsync();
    }

    private sealed class GroupRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "category", Order = 1)] public string? Category { get; set; }
        [ClickHouseColumn(Name = "amount", Order = 2)] public int Amount { get; set; }
    }

    /// <summary>Named projection class — anonymous types DON'T satisfy
    /// <c>where T : new()</c> (the C# compiler-generated ctor takes all
    /// properties as args, no parameterless one). Using a named class
    /// works around the LINQ-mapper constraint violation.</summary>
    public sealed class GroupAggregate
    {
        public string? Key { get; set; }
        public int Count { get; set; }
        public int Sum { get; set; }
    }

    [Fact]
    public async Task GroupByNullableKey_GroupsNullsTogether_LikeRawSql()
    {
        // Note: must use a named class (GroupAggregate) instead of an
        // anonymous type — the LINQ mapper has a `where T : new()`
        // constraint that compiler-generated anon types don't satisfy.
        // See AnonymousTypeProjection_AfterGroupBy_FailsConstraint test
        // below for the documented gap.
        var linqResults = await _conn.Table<GroupRow>(_table)
            .GroupBy(r => r.Category)
            .Select(g => new GroupAggregate
            {
                Key = g.Key,
                Count = g.Count(),
                Sum = g.Sum(r => r.Amount),
            })
            .ToListAsync();

        // Raw SQL oracle (use a list of tuples; Dictionary doesn't allow null keys).
        var oracle = new List<(string? key, int count, int sum)>();
        await foreach (var row in _conn.QueryAsync(
            $"SELECT category, count(), sum(amount) FROM {_table} GROUP BY category"))
        {
            oracle.Add(((string?)row[0], (int)Convert.ToInt64(row[1]), (int)Convert.ToInt64(row[2])));
        }

        _output.WriteLine($"LINQ groups: {linqResults.Count}; Oracle groups: {oracle.Count}");
        foreach (var g in linqResults)
            _output.WriteLine($"  LINQ: key={g.Key ?? "<null>"} count={g.Count} sum={g.Sum}");

        Assert.Equal(oracle.Count, linqResults.Count);

        foreach (var g in linqResults)
        {
            var match = oracle.FirstOrDefault(o => o.key == g.Key);
            Assert.True(oracle.Any(o => o.key == g.Key),
                $"LINQ produced key '{g.Key ?? "<null>"}' not in oracle");
            Assert.Equal(match.count, g.Count);
            Assert.Equal(match.sum, g.Sum);
        }

        // Specifically: the null group must aggregate all 3 null-category rows.
        var nullGroup = linqResults.FirstOrDefault(g => g.Key is null);
        Assert.NotNull(nullGroup);
        Assert.Equal(3, nullGroup!.Count);
        Assert.Equal(30 + 40 + 60, nullGroup.Sum);
    }

    [Fact]
    public async Task GroupBy_FilteredToNullKeyOnly_AggregatesCorrectly()
    {
        // Edge case: filter to only null keys, then group. The result is a
        // single group with all 3 null rows.
        var linqResults = await _conn.Table<GroupRow>(_table)
            .Where(r => r.Category == null)
            .GroupBy(r => r.Category)
            .Select(g => new GroupAggregate { Key = g.Key, Count = g.Count(), Sum = g.Sum(r => r.Amount) })
            .ToListAsync();

        Assert.Single(linqResults);
        Assert.Null(linqResults[0].Key);
        Assert.Equal(3, linqResults[0].Count);
        Assert.Equal(30 + 40 + 60, linqResults[0].Sum);
    }

    [Fact]
    public async Task AnonymousTypeProjection_AfterGroupBy_NowWorks()
    {
        // The constraint on MapAll<T> / QueryAsync<T> was relaxed from
        // `where T : new()` to `where T : class`. TypeMapper<T>'s args-ctor
        // strategy materializes anonymous types via their compiler-generated
        // constructor.
        var rows = await _conn.Table<GroupRow>(_table)
            .GroupBy(r => r.Category)
            .Select(g => new { Key = g.Key, Count = g.Count() })
            .ToListAsync();

        _output.WriteLine($"Anon-type LINQ groups: {rows.Count}");

        Assert.Equal(3, rows.Count);
        Assert.Contains(rows, r => r.Key == "A" && r.Count == 2);
        Assert.Contains(rows, r => r.Key == null && r.Count == 3);
        Assert.Contains(rows, r => r.Key == "B" && r.Count == 1);
    }

}
