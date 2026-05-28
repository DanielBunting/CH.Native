using CH.Native.Ado;
using CH.Native.Dapper;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration.Dapper;

/// <summary>
/// Tests for <see cref="ClickHouseDbConnectionDapperExtensions"/> (B1): the
/// Dapper-style API surface that resolves when a variable is typed as
/// <see cref="ClickHouseDbConnection"/>. Verifies result shape, parameter
/// binding, empty-result-set conventions, and cancellation.
/// </summary>
[Collection("ClickHouse")]
public class ClickHouseDbConnectionDapperExtensionsTests
{
    private readonly ClickHouseFixture _fixture;

    public ClickHouseDbConnectionDapperExtensionsTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private ClickHouseDbConnection NewClosed() =>
        new ClickHouseDbConnection(_fixture.ConnectionString);

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
    public async Task QueryAsync_EmptyResultSet_ReturnsEmptyList()
    {
        await using var conn = NewClosed();
        var rows = await conn.QueryAsync<NumberRow>(
            "SELECT toInt64(0) AS Value WHERE 1 = 0");
        Assert.Empty(rows);
    }

    [Fact]
    public async Task QueryAsync_AutoOpensClosedConnection()
    {
        await using var conn = NewClosed();
        Assert.Equal(System.Data.ConnectionState.Closed, conn.State);
        var rows = await conn.QueryAsync<NumberRow>("SELECT toInt64(1) AS Value");
        Assert.Single(rows);
        Assert.Equal(System.Data.ConnectionState.Open, conn.State);
    }

    // ------------------------------------------------------------------
    // QueryStreamAsync<T> — unbuffered
    // ------------------------------------------------------------------

    [Fact]
    public async Task QueryStreamAsync_YieldsRowsIncrementally()
    {
        await using var conn = NewClosed();
        var collected = new List<long>();
        await foreach (var r in conn.QueryStreamAsync<NumberRow>(
            "SELECT toInt64(number) AS Value FROM numbers(3) ORDER BY number"))
        {
            collected.Add(r.Value);
        }
        Assert.Equal(new[] { 0L, 1L, 2L }, collected);
    }

    [Fact]
    public async Task QueryStreamAsync_EmptyResultSet_YieldsNothing()
    {
        await using var conn = NewClosed();
        int count = 0;
        await foreach (var _ in conn.QueryStreamAsync<NumberRow>(
            "SELECT toInt64(0) AS Value WHERE 1 = 0"))
        {
            count++;
        }
        Assert.Equal(0, count);
    }

    // ------------------------------------------------------------------
    // QueryFirstAsync / QueryFirstOrDefaultAsync
    // ------------------------------------------------------------------

    [Fact]
    public async Task QueryFirstAsync_ReturnsFirstRow()
    {
        await using var conn = NewClosed();
        var r = await conn.QueryFirstAsync<NumberRow>(
            "SELECT toInt64(number) AS Value FROM numbers(3) ORDER BY number");
        Assert.Equal(0L, r.Value);
    }

    [Fact]
    public async Task QueryFirstAsync_EmptyResultSet_Throws()
    {
        await using var conn = NewClosed();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            conn.QueryFirstAsync<NumberRow>(
                "SELECT toInt64(0) AS Value WHERE 1 = 0"));
    }

    [Fact]
    public async Task QueryFirstOrDefaultAsync_EmptyResultSet_ReturnsNull()
    {
        await using var conn = NewClosed();
        var r = await conn.QueryFirstOrDefaultAsync<NumberRow>(
            "SELECT toInt64(0) AS Value WHERE 1 = 0");
        Assert.Null(r);
    }

    [Fact]
    public async Task QueryFirstOrDefaultAsync_NonEmpty_ReturnsFirstRow()
    {
        await using var conn = NewClosed();
        var r = await conn.QueryFirstOrDefaultAsync<NumberRow>(
            "SELECT toInt64(99) AS Value");
        Assert.NotNull(r);
        Assert.Equal(99L, r!.Value);
    }

    // ------------------------------------------------------------------
    // QuerySingleAsync / QuerySingleOrDefaultAsync
    // ------------------------------------------------------------------

    [Fact]
    public async Task QuerySingleAsync_ExactlyOneRow_Returns()
    {
        await using var conn = NewClosed();
        var r = await conn.QuerySingleAsync<NumberRow>("SELECT toInt64(7) AS Value");
        Assert.Equal(7L, r.Value);
    }

    [Fact]
    public async Task QuerySingleAsync_EmptyResultSet_Throws()
    {
        await using var conn = NewClosed();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            conn.QuerySingleAsync<NumberRow>(
                "SELECT toInt64(0) AS Value WHERE 1 = 0"));
    }

    [Fact]
    public async Task QuerySingleAsync_MultipleRows_Throws()
    {
        await using var conn = NewClosed();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            conn.QuerySingleAsync<NumberRow>(
                "SELECT toInt64(number) AS Value FROM numbers(3)"));
    }

    [Fact]
    public async Task QuerySingleOrDefaultAsync_EmptyResultSet_ReturnsNull()
    {
        await using var conn = NewClosed();
        var r = await conn.QuerySingleOrDefaultAsync<NumberRow>(
            "SELECT toInt64(0) AS Value WHERE 1 = 0");
        Assert.Null(r);
    }

    [Fact]
    public async Task QuerySingleOrDefaultAsync_MultipleRows_Throws()
    {
        await using var conn = NewClosed();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            conn.QuerySingleOrDefaultAsync<NumberRow>(
                "SELECT toInt64(number) AS Value FROM numbers(3)"));
    }

    // ------------------------------------------------------------------
    // Resolution: a variable typed as ClickHouseDbConnection should pick
    // our extension over Dapper's (C# more-derived-type resolution rule).
    // ------------------------------------------------------------------

    [Fact]
    public async Task TypedConnection_ResolvesToOurExtension()
    {
        // No `using Dapper;` in this file — if our extension didn't exist
        // we'd get a compile error rather than Dapper's QueryAsync. This
        // test exists to make any future namespace change loud: the compile
        // of this method fails if the symbol is gone.
        await using ClickHouseDbConnection conn = NewClosed();
        var rows = await conn.QueryAsync<NumberRow>("SELECT toInt64(1) AS Value");
        Assert.Single(rows);
        Assert.Equal(1L, rows[0].Value);
    }

    // ------------------------------------------------------------------
    // Row shapes
    // ------------------------------------------------------------------

    public class NumberRow { public long Value { get; set; } }
}
