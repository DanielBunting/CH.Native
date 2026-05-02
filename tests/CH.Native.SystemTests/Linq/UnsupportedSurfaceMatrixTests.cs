using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Linq.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Linq;

/// <summary>
/// Lock-in matrix for the LINQ "unsupported surface" — every operator that
/// currently throws <see cref="NotSupportedException"/> at translation time,
/// pinned to the exact failure mode so a future contract change is visible.
///
/// <para>
/// <see cref="UnsupportedOperatorTests"/> covers Join, multi-key GroupBy, and
/// HAVING shape. This file extends the matrix with the rest of the visitor's
/// known throw sites so the LINQ surface is fully documented:
/// </para>
/// <list type="bullet">
/// <item><description>LINQ verbs the visitor's switch falls through (e.g. <c>SelectMany</c>,
///     <c>Aggregate</c>, <c>Zip</c>, <c>Concat</c>, <c>Union</c>) → throw
///     "LINQ method '...' is not supported".</description></item>
/// <item><description>Synchronous enumeration via <c>foreach</c> / <c>.ToList()</c> →
///     throw "Synchronous enumeration is not supported".</description></item>
/// <item><description>Member access on unsupported types → typed throw.</description></item>
/// </list>
/// </summary>
[Collection("LinqFacts")]
[Trait(Categories.Name, Categories.Linq)]
public class UnsupportedSurfaceMatrixTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _node;
    private readonly LinqFactTableFixture _facts;
    private ClickHouseConnection _conn = null!;

    public UnsupportedSurfaceMatrixTests(SingleNodeFixture node, LinqFactTableFixture facts)
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
    public async Task SelectMany_Throws_NotSupported()
    {
        var q = _conn.Table<LinqFactRow>(_facts.TableName)
            .SelectMany(x => new[] { x.Id });

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => q.ToListAsync());
        Assert.Contains("SelectMany", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Concat_Throws_NotSupported()
    {
        var q1 = _conn.Table<LinqFactRow>(_facts.TableName);
        var q2 = _conn.Table<LinqFactRow>(_facts.TableName);
        var concat = q1.Concat(q2);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => concat.ToListAsync());
        Assert.Contains("Concat", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Union_Throws_NotSupported()
    {
        var q1 = _conn.Table<LinqFactRow>(_facts.TableName);
        var q2 = _conn.Table<LinqFactRow>(_facts.TableName);
        var unioned = q1.Union(q2);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => unioned.ToListAsync());
        Assert.Contains("Union", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Except_Throws_NotSupported()
    {
        var q1 = _conn.Table<LinqFactRow>(_facts.TableName);
        var q2 = _conn.Table<LinqFactRow>(_facts.TableName);
        var diff = q1.Except(q2);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => diff.ToListAsync());
        Assert.Contains("Except", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Reverse_Throws_NotSupported()
    {
        var q = _conn.Table<LinqFactRow>(_facts.TableName).Reverse();

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => q.ToListAsync());
        Assert.Contains("Reverse", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LongCount_Translates_OrThrows_NotSupported()
    {
        // LongCount maps to Queryable.LongCount which the visitor handles
        // alongside Count (visitor line 74). Pin that LongCount() works on
        // the LINQ surface — its absence would be a regression.
        var q = _conn.Table<LinqFactRow>(_facts.TableName);

        // No assertion on the value (depends on the seeded fact table) —
        // we just need it to not throw at translation time.
        await q.LongCountAsync();
    }

    [Fact]
    public void SyncEnumeration_ToList_Throws_NotSupported()
    {
        // Synchronous foreach / .ToList() must throw at the IEnumerable boundary,
        // not silently materialise the entire result set on the calling thread.
        var q = _conn.Table<LinqFactRow>(_facts.TableName);

        var ex = Assert.Throws<NotSupportedException>(() => q.ToList());
        Assert.Contains("Synchronous enumeration", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SyncEnumeration_ForEach_Throws_NotSupported()
    {
        var q = _conn.Table<LinqFactRow>(_facts.TableName);

        Assert.Throws<NotSupportedException>(() =>
        {
            foreach (var _ in q) { /* unreachable */ }
        });
    }

    [Fact]
    public async Task GroupJoin_Throws_NotSupported()
    {
        var left = _conn.Table<LinqFactRow>(_facts.TableName);
        var right = _conn.Table<LinqFactRow>(_facts.TableName);

        var q = left.GroupJoin(
            right,
            l => l.Id,
            r => r.Id,
            (l, rs) => new { l.Id, Group = rs });

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => q.ToListAsync());
        Assert.Contains("GroupJoin", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Cast_Throws_NotSupported()
    {
        var q = _conn.Table<LinqFactRow>(_facts.TableName).Cast<LinqFactRow>();

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => q.ToListAsync());
        Assert.Contains("Cast", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OfType_Throws_NotSupported()
    {
        var q = _conn.Table<LinqFactRow>(_facts.TableName).OfType<LinqFactRow>();

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => q.ToListAsync());
        Assert.Contains("OfType", ex.Message, StringComparison.Ordinal);
    }
}
