using CH.Native.Connection;
using CH.Native.Dapper;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration.Dapper;

/// <summary>
/// Tests for <see cref="ClickHouseConnectionDapperExtensions"/>: the sibling
/// of <see cref="ClickHouseDbConnectionDapperExtensions"/> that binds when a
/// variable is typed as <see cref="ClickHouseConnection"/> — the shape returned
/// by <c>ClickHouseDataSource.OpenConnectionAsync</c>. Verifies result shape,
/// parameter binding, empty-result-set conventions, and cancellation.
/// </summary>
[Collection("ClickHouse")]
public class ClickHouseConnectionDapperExtensionsTests
{
    private readonly ClickHouseFixture _fixture;

    public ClickHouseConnectionDapperExtensionsTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private ClickHouseConnection NewClosed() =>
        new ClickHouseConnection(_fixture.ConnectionString);

    // ------------------------------------------------------------------
    // QueryAsync<T> — buffered
    // ------------------------------------------------------------------

    [Fact]
    public async Task QueryAsync_NoParams_ReturnsAllRows()
    {
        await using var conn = NewClosed();
        var rows = await conn.QueryAsync<NumberRow>(
            "SELECT toInt64(number) AS Value FROM numbers(5) ORDER BY number");
        Assert.Equal(5, rows.Count);
        Assert.Equal(new[] { 0L, 1L, 2L, 3L, 4L }, rows.Select(r => r.Value));
    }

    [Fact]
    public async Task QueryAsync_AnonymousParams_BindCorrectly()
    {
        await using var conn = NewClosed();
        var rows = await conn.QueryAsync<NumberRow>(
            "SELECT toInt64(number) AS Value FROM numbers(10) WHERE number >= @min AND number < @max ORDER BY number",
            new { min = 3, max = 7 });
        Assert.Equal(new[] { 3L, 4L, 5L, 6L }, rows.Select(r => r.Value));
    }

    [Fact]
    public async Task QueryAsync_DictionaryParams_BindCorrectly()
    {
        await using var conn = NewClosed();
        var dict = new Dictionary<string, object?> { ["v"] = 42 };
        var rows = await conn.QueryAsync<NumberRow>(
            "SELECT toInt64(@v) AS Value", dict);
        Assert.Single(rows);
        Assert.Equal(42L, rows[0].Value);
    }

    [Fact]
    public async Task QueryAsync_OpensClosedConnectionOnDemand()
    {
        await using var conn = NewClosed();
        // Deliberately do not call OpenAsync; the extension must lift it open.
        var rows = await conn.QueryAsync<NumberRow>("SELECT toInt64(1) AS Value");
        Assert.Equal(1L, rows.Single().Value);
    }

    // ------------------------------------------------------------------
    // First / Single semantics
    // ------------------------------------------------------------------

    [Fact]
    public async Task QueryFirstAsync_NonEmpty_ReturnsFirstRow()
    {
        await using var conn = NewClosed();
        var row = await conn.QueryFirstAsync<NumberRow>("SELECT toInt64(42) AS Value");
        Assert.Equal(42L, row.Value);
    }

    [Fact]
    public async Task QueryFirstAsync_Empty_Throws()
    {
        await using var conn = NewClosed();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            conn.QueryFirstAsync<NumberRow>("SELECT toInt64(0) AS Value WHERE 1 = 0"));
    }

    [Fact]
    public async Task QueryFirstOrDefaultAsync_Empty_ReturnsNull()
    {
        await using var conn = NewClosed();
        var row = await conn.QueryFirstOrDefaultAsync<NumberRow>(
            "SELECT toInt64(0) AS Value WHERE 1 = 0");
        Assert.Null(row);
    }

    [Fact]
    public async Task QuerySingleAsync_TwoRows_Throws()
    {
        await using var conn = NewClosed();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            conn.QuerySingleAsync<NumberRow>(
                "SELECT toInt64(number) AS Value FROM numbers(2)"));
    }

    [Fact]
    public async Task QuerySingleOrDefaultAsync_Empty_ReturnsNull()
    {
        await using var conn = NewClosed();
        var row = await conn.QuerySingleOrDefaultAsync<NumberRow>(
            "SELECT toInt64(0) AS Value WHERE 1 = 0");
        Assert.Null(row);
    }

    // ------------------------------------------------------------------
    // Cancellation
    // ------------------------------------------------------------------

    [Fact]
    public async Task QueryAsync_HonoursCancellationToken()
    {
        await using var conn = NewClosed();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            conn.QueryAsync<NumberRow>(
                "SELECT toInt64(1) AS Value",
                cancellationToken: cts.Token));
    }

    public class NumberRow { public long Value { get; set; } }
}
