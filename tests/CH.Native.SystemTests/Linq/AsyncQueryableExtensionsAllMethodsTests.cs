using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Linq.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Linq;

/// <summary>
/// Per-method coverage of every public extension on
/// <see cref="AsyncQueryableExtensions"/>. Each method is exercised end-to-end
/// against the seeded fact table; the assertion compares against an oracle
/// computed from the in-memory <see cref="LinqFactTableFixture.Rows"/> list
/// so the test is independent of the SQL the provider emits.
/// </summary>
[Collection("LinqFacts")]
[Trait(Categories.Name, Categories.Linq)]
public class AsyncQueryableExtensionsAllMethodsTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _node;
    private readonly LinqFactTableFixture _facts;
    private ClickHouseConnection _conn = null!;

    public AsyncQueryableExtensionsAllMethodsTests(SingleNodeFixture node, LinqFactTableFixture facts)
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

    private IQueryable<LinqFactRow> Table() => _conn.Table<LinqFactRow>(_facts.TableName);

    // --- Materialisation ---

    [Fact]
    public async Task ToListAsync_ReturnsAllRows()
    {
        var actual = await Table().ToListAsync();
        Assert.Equal(_facts.Rows.Count, actual.Count);
    }

    [Fact]
    public async Task ToArrayAsync_ReturnsAllRows()
    {
        var actual = await Table().ToArrayAsync();
        Assert.Equal(_facts.Rows.Count, actual.Length);
    }

    // --- First / FirstOrDefault ---

    [Fact]
    public async Task FirstAsync_NoPredicate_ReturnsFirstByOrderBy()
    {
        var first = await Table().OrderBy(r => r.Id).FirstAsync();
        Assert.Equal(_facts.Rows.Min(r => r.Id), first.Id);
    }

    [Fact]
    public async Task FirstAsync_WithPredicate_AppliesFilter()
    {
        var first = await Table().OrderBy(r => r.Id).FirstAsync(r => r.Country == "DE");
        Assert.Equal("DE", first.Country);
    }

    [Fact]
    public async Task FirstAsync_NoMatch_ThrowsInvalidOperation()
    {
        // Mirror Enumerable.First semantics — InvalidOperationException on empty result.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Table().FirstAsync(r => r.Country == "ZZ"));
    }

    [Fact]
    public async Task FirstOrDefaultAsync_NoPredicate_ReturnsRow()
    {
        var first = await Table().OrderBy(r => r.Id).FirstOrDefaultAsync();
        Assert.NotNull(first);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_NoMatch_ReturnsNull()
    {
        var first = await Table().FirstOrDefaultAsync(r => r.Country == "ZZ");
        Assert.Null(first);
    }

    // --- Single / SingleOrDefault ---

    [Fact]
    public async Task SingleAsync_WithPredicate_ReturnsExactlyOne()
    {
        var single = await Table().SingleAsync(r => r.Id == 1);
        Assert.Equal(1, single.Id);
    }

    [Fact]
    public async Task SingleAsync_MultipleMatch_Throws()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Table().SingleAsync(r => r.Country == "DE"));
    }

    [Fact]
    public async Task SingleAsync_NoMatch_Throws()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Table().SingleAsync(r => r.Country == "ZZ"));
    }

    [Fact]
    public async Task SingleOrDefaultAsync_NoMatch_ReturnsNull()
    {
        var result = await Table().SingleOrDefaultAsync(r => r.Country == "ZZ");
        Assert.Null(result);
    }

    [Fact]
    public async Task SingleOrDefaultAsync_OneMatch_ReturnsRow()
    {
        var result = await Table().SingleOrDefaultAsync(r => r.Id == 42);
        Assert.NotNull(result);
        Assert.Equal(42, result!.Id);
    }

    // --- Count / LongCount ---

    [Fact]
    public async Task CountAsync_NoPredicate_ReturnsTotalRows()
    {
        var count = await Table().CountAsync();
        Assert.Equal(_facts.Rows.Count, count);
    }

    [Fact]
    public async Task CountAsync_WithPredicate_AppliesFilter()
    {
        var count = await Table().CountAsync(r => r.Country == "DE");
        Assert.Equal(_facts.Rows.Count(r => r.Country == "DE"), count);
    }

    [Fact]
    public async Task LongCountAsync_NoPredicate_ReturnsTotalRows()
    {
        var count = await Table().LongCountAsync();
        Assert.Equal((long)_facts.Rows.Count, count);
    }

    // --- Any / All ---

    [Fact]
    public async Task AnyAsync_NoPredicate_True_WhenRowsExist()
    {
        Assert.True(await Table().AnyAsync());
    }

    [Fact]
    public async Task AnyAsync_WithPredicate_True_WhenAnyRowMatches()
    {
        Assert.True(await Table().AnyAsync(r => r.Country == "DE"));
    }

    [Fact]
    public async Task AnyAsync_WithPredicate_False_WhenNoRowMatches()
    {
        Assert.False(await Table().AnyAsync(r => r.Country == "ZZ"));
    }

    [Fact]
    public async Task AllAsync_True_WhenAllRowsMatch()
    {
        Assert.True(await Table().AllAsync(r => r.Id > 0));
    }

    [Fact]
    public async Task AllAsync_False_WhenAnyRowDoesNotMatch()
    {
        Assert.False(await Table().AllAsync(r => r.Country == "DE"));
    }

    // --- Aggregates: Sum / Average / Min / Max ---

    [Fact]
    public async Task SumAsync_OverDouble_ReturnsExpectedSum()
    {
        var sum = await Table().SumAsync(r => r.Amount);
        var expected = _facts.Rows.Sum(r => r.Amount);
        Assert.Equal(expected, sum, precision: 2);
    }

    [Fact]
    public async Task AverageAsync_OverInt_ReturnsExpectedAverage()
    {
        var avg = await Table().AverageAsync(r => r.Quantity);
        var expected = _facts.Rows.Average(r => r.Quantity);
        Assert.Equal(expected, avg, precision: 5);
    }

    [Fact]
    public async Task MinAsync_OverInt_ReturnsExpectedMin()
    {
        var min = await Table().MinAsync(r => r.Quantity);
        Assert.Equal(_facts.Rows.Min(r => r.Quantity), min);
    }

    [Fact]
    public async Task MaxAsync_OverInt_ReturnsExpectedMax()
    {
        var max = await Table().MaxAsync(r => r.Quantity);
        Assert.Equal(_facts.Rows.Max(r => r.Quantity), max);
    }

    // --- Cancellation ---

    [Fact]
    public async Task ToListAsync_CancelledToken_Throws()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await Table().ToListAsync(cts.Token));
    }

    [Fact]
    public async Task CountAsync_CancelledToken_Throws()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await Table().CountAsync(cts.Token));
    }

    [Fact]
    public async Task FirstAsync_CancelledToken_Throws()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await Table().FirstAsync(cts.Token));
    }
}
