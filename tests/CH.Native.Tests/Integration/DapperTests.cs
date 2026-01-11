using CH.Native.Ado;
using CH.Native.Tests.Fixtures;
using Dapper;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class DapperTests
{
    private readonly ClickHouseFixture _fixture;

    public DapperTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Query_ReturnsTypedResults()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.QueryAsync<NumberResult>(
            "SELECT toUInt64(number) as Value FROM numbers(5)");

        var list = results.ToList();
        Assert.Equal(5, list.Count);
        Assert.Equal(new ulong[] { 0, 1, 2, 3, 4 }, list.Select(r => r.Value).ToArray());
    }

    [Fact]
    public async Task Query_WithParameters_WorksCorrectly()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.QueryAsync<NumberResult>(
            "SELECT toUInt64(number) as Value FROM numbers(10) WHERE number >= @min AND number < @max",
            new { min = 3, max = 7 });

        var list = results.ToList();
        Assert.Equal(4, list.Count);
        Assert.Equal(new ulong[] { 3, 4, 5, 6 }, list.Select(r => r.Value).ToArray());
    }

    [Fact]
    public async Task QueryFirst_ReturnsSingleResult()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.QueryFirstAsync<ScalarResult>(
            "SELECT toInt32(42) as Value");

        Assert.Equal(42, result.Value);
    }

    [Fact]
    public async Task QueryFirstOrDefault_ReturnsNullForEmptyResult()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.QueryFirstOrDefaultAsync<ScalarResult>(
            "SELECT toInt32(1) as Value WHERE 1 = 0");

        Assert.Null(result);
    }

    [Fact]
    public async Task QuerySingle_ReturnsSingleResult()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.QuerySingleAsync<StringResult>(
            "SELECT 'hello' as Value");

        Assert.Equal("hello", result.Value);
    }

    [Fact]
    public async Task Execute_RunsCommand()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Create table
        await connection.ExecuteAsync(@"
            CREATE TABLE IF NOT EXISTS test_dapper_execute (
                id UInt32,
                name String
            ) ENGINE = Memory");

        // Verify table exists
        var tables = await connection.QueryAsync<TableResult>(
            "SELECT name as Name FROM system.tables WHERE database = currentDatabase() AND name = 'test_dapper_execute'");

        Assert.Single(tables);

        // Cleanup
        await connection.ExecuteAsync("DROP TABLE IF EXISTS test_dapper_execute");
    }

    [Fact]
    public async Task Execute_WithParameters_WorksCorrectly()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Create table
        await connection.ExecuteAsync(@"
            CREATE TABLE IF NOT EXISTS test_dapper_insert (
                id UInt32,
                name String
            ) ENGINE = Memory");

        // Insert with parameters - note: ClickHouse doesn't support parameterized INSERT VALUES directly
        // but we can use it in a SELECT that generates data
        await connection.ExecuteAsync(
            "INSERT INTO test_dapper_insert SELECT toUInt32(@id), @name",
            new { id = 1, name = "test" });

        // Verify
        var result = await connection.QueryFirstAsync<NameResult>(
            "SELECT name as Name FROM test_dapper_insert WHERE id = @id",
            new { id = 1 });

        Assert.Equal("test", result.Name);

        // Cleanup
        await connection.ExecuteAsync("DROP TABLE IF EXISTS test_dapper_insert");
    }

    [Fact]
    public async Task Query_WithMultipleColumns_MapsCorrectly()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.QueryAsync<UserResult>(@"
            SELECT
                toUInt32(1) as Id,
                'Alice' as Name,
                toInt32(30) as Age,
                true as IsActive");

        var user = results.Single();
        Assert.Equal(1U, user.Id);
        Assert.Equal("Alice", user.Name);
        Assert.Equal(30, user.Age);
        Assert.True(user.IsActive);
    }

    [Fact]
    public async Task Query_WithNullableColumn_HandlesNull()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.QueryAsync<NullableResult>(@"
            SELECT
                CAST(42 AS Nullable(Int32)) as Value
            UNION ALL
            SELECT
                CAST(NULL AS Nullable(Int32)) as Value");

        var list = results.ToList();
        Assert.Equal(2, list.Count);
        // UNION ALL doesn't guarantee order, so check contents without assuming order
        Assert.Contains(list, r => r.Value == 42);
        Assert.Contains(list, r => r.Value == null);
    }

    [Fact]
    public async Task ExecuteScalar_ReturnsValue()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<int>(
            "SELECT toInt32(count()) FROM numbers(100)");

        Assert.Equal(100, result);
    }

    [Fact]
    public async Task Query_WithStringParameters_EscapesCorrectly()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Test SQL injection prevention
        var result = await connection.QueryFirstAsync<StringResult>(
            "SELECT @value as Value",
            new { value = "O'Brien" });

        Assert.Equal("O'Brien", result.Value);
    }

    [Fact]
    public async Task Query_WithDateTimeParameter_WorksCorrectly()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var testDate = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
        var result = await connection.QueryFirstAsync<DateResult>(
            "SELECT @date as Value",
            new { date = testDate });

        Assert.Equal(testDate, result.Value);
    }

    [Fact]
    public async Task Query_WithGuidParameter_WorksCorrectly()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var testGuid = Guid.Parse("12345678-1234-1234-1234-123456789012");
        var result = await connection.QueryFirstAsync<GuidResult>(
            "SELECT @id as Value",
            new { id = testGuid });

        Assert.Equal(testGuid, result.Value);
    }

    #region Array Parameter Tests

    // IMPORTANT: Array parameters via Dapper's anonymous object syntax are NOT supported.
    //
    // This is a Dapper limitation, not a CH.Native limitation. When Dapper sees an array
    // in an anonymous object, it performs "inline expansion" for SQL IN clause compatibility:
    //   Original: @ids with ids = [1, 2, 3]
    //   Expanded: (@ids0, @ids1, @ids2) - creates a TUPLE, not a ClickHouse array
    //
    // WORKAROUNDS:
    //
    // 1. Use direct ADO.NET (recommended for arrays):
    //    using var cmd = connection.CreateCommand();
    //    cmd.CommandText = "SELECT count() FROM t WHERE hasAny([id], @ids)";
    //    cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "ids", Value = new[] { 1, 2, 3 } });
    //    var result = await cmd.ExecuteScalarAsync();
    //
    // 2. Use the native CH.Native API directly:
    //    await connection.Inner.ExecuteScalarAsync<long>(
    //        "SELECT count() FROM t WHERE hasAny([id], @ids)",
    //        new ClickHouseParameterCollection { { "ids", new[] { 1, 2, 3 } } });

    [Fact(Skip = "Dapper inline expands arrays to tuples - use direct ADO.NET for array params (see workarounds above)")]
    public async Task Query_WithArrayParameter_FiltersCorrectly()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Use hasAny to check if value is in array parameter
        var ids = new int[] { 2, 4, 6, 8 };
        var results = await connection.QueryAsync<NumberResult>(
            "SELECT toUInt64(number) as Value FROM numbers(10) WHERE hasAny([number], @ids)",
            new { ids });

        var list = results.ToList();
        Assert.Equal(4, list.Count);
        Assert.Equal(new ulong[] { 2, 4, 6, 8 }, list.Select(r => r.Value).OrderBy(x => x).ToArray());
    }

    [Fact(Skip = "Dapper inline expands arrays to tuples - use direct ADO.NET for array params (see workarounds above)")]
    public async Task Query_WithStringArrayParameter_WorksCorrectly()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var names = new string[] { "Alice", "Bob", "Charlie" };
        var result = await connection.QueryFirstAsync<ScalarResult>(
            "SELECT toInt32(length(@names)) as Value",
            new { names });

        Assert.Equal(3, result.Value);
    }

    #endregion

    #region Empty Result Tests

    [Fact]
    public async Task QueryFirstOrDefault_EmptyResult_ReturnsDefault()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.QueryFirstOrDefaultAsync<ScalarResult>(
            "SELECT toInt32(1) as Value WHERE 1 = 0");

        Assert.Null(result);
    }

    [Fact]
    public async Task Query_EmptyResult_ReturnsEmptyEnumerable()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.QueryAsync<ScalarResult>(
            "SELECT toInt32(number) as Value FROM numbers(0)");

        Assert.Empty(results);
    }

    #endregion

    #region Connection Reuse Tests

    [Fact]
    public async Task Dapper_MultipleQueries_SameConnection()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Multiple Dapper queries on same connection
        var r1 = await connection.QueryFirstAsync<ScalarResult>("SELECT toInt32(1) as Value");
        var r2 = await connection.QueryFirstAsync<ScalarResult>("SELECT toInt32(2) as Value");
        var r3 = await connection.QueryFirstAsync<ScalarResult>("SELECT toInt32(3) as Value");

        Assert.Equal(1, r1.Value);
        Assert.Equal(2, r2.Value);
        Assert.Equal(3, r3.Value);
    }

    [Fact]
    public async Task Dapper_QueryAndExecute_Interleaved()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Create table
        await connection.ExecuteAsync(@"
            CREATE TABLE IF NOT EXISTS test_dapper_interleave (
                id UInt32
            ) ENGINE = Memory");

        // Query
        var count1 = await connection.QueryFirstAsync<ScalarResult>(
            "SELECT toInt32(count()) as Value FROM test_dapper_interleave");
        Assert.Equal(0, count1.Value);

        // Insert via SELECT
        await connection.ExecuteAsync(
            "INSERT INTO test_dapper_interleave SELECT toUInt32(number) FROM numbers(5)");

        // Query again
        var count2 = await connection.QueryFirstAsync<ScalarResult>(
            "SELECT toInt32(count()) as Value FROM test_dapper_interleave");
        Assert.Equal(5, count2.Value);

        // Cleanup
        await connection.ExecuteAsync("DROP TABLE IF EXISTS test_dapper_interleave");
    }

    #endregion

    #region Dynamic Query Tests

    [Fact]
    public async Task Query_Dynamic_ReturnsExpandoObjects()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.QueryAsync(
            "SELECT toInt32(1) as id, 'test' as name");

        var list = results.ToList();
        Assert.Single(list);

        dynamic row = list[0];
        Assert.Equal(1, (int)row.id);
        Assert.Equal("test", (string)row.name);
    }

    #endregion

    #region Large Result Tests

    [Fact]
    public async Task Query_LargeResult_Streams()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Dapper with buffered: false would be ideal but default buffered works too
        var results = await connection.QueryAsync<NumberResult>(
            "SELECT toUInt64(number) as Value FROM numbers(50000)");

        var count = results.Count();
        Assert.Equal(50000, count);
    }

    #endregion

    #region Result Types

    private class NumberResult
    {
        public ulong Value { get; set; }
    }

    private class ScalarResult
    {
        public int Value { get; set; }
    }

    private class StringResult
    {
        public string Value { get; set; } = "";
    }

    private class TableResult
    {
        public string Name { get; set; } = "";
    }

    private class NameResult
    {
        public string Name { get; set; } = "";
    }

    private class UserResult
    {
        public uint Id { get; set; }
        public string Name { get; set; } = "";
        public int Age { get; set; }
        public bool IsActive { get; set; }
    }

    private class NullableResult
    {
        public int? Value { get; set; }
    }

    private class DateResult
    {
        public DateTime Value { get; set; }
    }

    private class GuidResult
    {
        public Guid Value { get; set; }
    }

    #endregion
}
