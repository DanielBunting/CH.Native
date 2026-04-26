using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Linq.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Linq;

[Collection("LinqFacts")]
[Trait(Categories.Name, Categories.Linq)]
public class NullSemanticsTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _node;
    private readonly LinqFactTableFixture _facts;
    private ClickHouseConnection _conn = null!;

    public NullSemanticsTests(SingleNodeFixture node, LinqFactTableFixture facts)
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
    public async Task Where_NullableEqualsValue_ExcludesNulls()
    {
        // Three-valued logic: NULL = 5 yields NULL, which is filtered out.
        var ids = await _conn.Table<LinqFactRow>(_facts.TableName)
            .Where(x => x.OptionalCode == 5)
            .Select(x => x.Id)
            .ToListAsync();

        long rawCount = await LinqAssertions.ExecuteScalarAsync<long>(
            _conn,
            $"SELECT count() FROM {_facts.TableName} WHERE optional_code = 5");

        Assert.Equal((int)rawCount, ids.Count);
        // No NULL row may sneak in: every returned row must have OptionalCode == 5.
        Assert.All(ids, id => Assert.Contains(_facts.Rows, r => r.Id == id && r.OptionalCode == 5));
    }

    [Fact]
    public async Task Where_NullableIsNull_FindsOnlyNulls()
    {
        // x => x.OptionalCode == null must translate to IS NULL, not "= NULL"
        // (the latter always evaluates to NULL/false in SQL).
        var ids = await _conn.Table<LinqFactRow>(_facts.TableName)
            .Where(x => x.OptionalCode == null)
            .Select(x => x.Id)
            .ToListAsync();

        long expected = _facts.Rows.Count(r => r.OptionalCode is null);
        Assert.Equal((int)expected, ids.Count);
        Assert.True(expected > 0, "Fixture must seed at least one NULL row");
    }

    [Fact]
    public async Task Select_NullableArithmetic_PropagatesNull()
    {
        // NULL + 1 = NULL on the server; result list should preserve NULLs.
        var values = await _conn.Table<LinqFactRow>(_facts.TableName)
            .OrderBy(x => x.Id)
            .Select(x => x.OptionalCode + 1)
            .ToListAsync();

        Assert.Equal(_facts.Rows.Count, values.Count);

        var seededOrdered = _facts.Rows.OrderBy(r => r.Id).ToList();
        for (int i = 0; i < values.Count; i++)
        {
            int? expected = seededOrdered[i].OptionalCode + 1;
            Assert.Equal(expected, values[i]);
        }
    }

    [Fact]
    public async Task OrderBy_Nullable_NullPlacement()
    {
        // Pin ClickHouse default NULL ordering: NULLs sort last under ASC.
        var ordered = await _conn.Table<LinqFactRow>(_facts.TableName)
            .OrderBy(x => x.OptionalCode)
            .ThenBy(x => x.Id)
            .Select(x => x.OptionalCode)
            .ToListAsync();

        // Build the same expectation from the seed and compare.
        var oracle = await ReadColumnAsync<int?>(
            _conn,
            $"SELECT optional_code FROM {_facts.TableName} ORDER BY optional_code, id");

        Assert.Equal(oracle.Count, ordered.Count);
        for (int i = 0; i < oracle.Count; i++)
        {
            Assert.Equal(oracle[i], ordered[i]);
        }
    }

    private static async Task<List<T?>> ReadColumnAsync<T>(ClickHouseConnection conn, string sql)
    {
        var result = new List<T?>();
        await foreach (var row in conn.QueryAsync(sql))
        {
            object? v = row[0];
            if (v is null)
                result.Add(default);
            else
                result.Add((T)v);
        }
        return result;
    }
}
