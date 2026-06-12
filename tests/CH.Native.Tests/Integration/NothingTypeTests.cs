using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

// The Nothing type is what a bare `SELECT NULL` (Nullable(Nothing)) or `SELECT []`
// (Array(Nothing)) produces. ORMs and health checks emit these probes, so the reader
// must answer them and — equally important — leave the connection healthy afterwards
// (an unsupported type thrown mid-block poisons the pipe for the next query).
[Collection("ClickHouse")]
public class NothingTypeTests
{
    private readonly ClickHouseFixture _fixture;

    public NothingTypeTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task<List<object?[]>> QueryAsync(ClickHouseConnection connection, string sql)
    {
        var rows = new List<object?[]>();
        await foreach (var row in connection.QueryStreamAsync(sql))
        {
            var values = new object?[row.FieldCount];
            for (int i = 0; i < row.FieldCount; i++)
            {
                values[i] = row[i];
            }
            rows.Add(values);
        }
        return rows;
    }

    [Fact]
    public async Task SelectNull_ReturnsSingleNullRow()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var rows = await QueryAsync(connection, "SELECT NULL");

        var row = Assert.Single(rows);
        Assert.Null(Assert.Single(row));
    }

    [Fact]
    public async Task SelectEmptyArray_ReturnsEmptyArray()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var rows = await QueryAsync(connection, "SELECT []");

        var value = Assert.Single(Assert.Single(rows));
        var array = Assert.IsAssignableFrom<System.Collections.IEnumerable>(value);
        Assert.Empty(array.Cast<object?>());
    }

    [Fact]
    public async Task SelectNull_MultiRow_AllNull()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var rows = await QueryAsync(connection, "SELECT NULL FROM numbers(5)");

        Assert.Equal(5, rows.Count);
        Assert.All(rows, r => Assert.Null(Assert.Single(r)));
    }

    [Fact]
    public async Task SelectNull_MixedWithOtherColumns()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var rows = await QueryAsync(connection, "SELECT 1, NULL, 'x'");

        var row = Assert.Single(rows);
        Assert.Equal(3, row.Length);
        Assert.Null(row[1]);
        Assert.Equal("x", row[2]);
    }

    [Fact]
    public async Task SelectNullIf_NothingTyped_Expression()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // arrayElement over Array(Nullable(Nothing)) yields Nullable(Nothing).
        // (A bare non-nullable Nothing projection like `SELECT [][1]` is rejected by the
        // server itself — "Invalid projection column with type Nothing".)
        var rows = await QueryAsync(connection, "SELECT [NULL][1]");

        Assert.Null(Assert.Single(Assert.Single(rows)));
    }

    [Fact]
    public async Task ExecuteScalar_SelectNull_ReturnsNull()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<object?>("SELECT NULL");

        Assert.Null(result);
    }

    // The historical failure mode was worse than the NotSupportedException itself: the
    // reader threw mid-block and poisoned the connection for the NEXT query. Prove the
    // same connection keeps working after Nothing-typed results.
    [Fact]
    public async Task ConnectionRemainsUsable_AfterNothingTypedQueries()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await QueryAsync(connection, "SELECT NULL");
        await QueryAsync(connection, "SELECT []");
        var result = await connection.ExecuteScalarAsync<int>("SELECT 42");

        Assert.Equal(42, result);
    }

    [Fact]
    public async Task SelectNull_LazyStringMaterialization_Works()
    {
        await using var connection = new ClickHouseConnection(
            _fixture.ConnectionString + ";StringMaterialization=Lazy");
        await connection.OpenAsync();

        var rows = await QueryAsync(connection, "SELECT NULL, 'text'");

        var row = Assert.Single(rows);
        Assert.Null(row[0]);
        Assert.Equal("text", row[1]);
    }
}
