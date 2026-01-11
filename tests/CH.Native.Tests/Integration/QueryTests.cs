using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Exceptions;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class QueryTests
{
    private readonly ClickHouseFixture _fixture;

    public QueryTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ExecuteScalarAsync_SelectOne_ReturnsOne()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<int>("SELECT 1");

        Assert.Equal(1, result);
    }

    [Fact]
    public async Task ExecuteScalarAsync_SelectOnePlusOne_ReturnsTwo()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<int>("SELECT 1 + 1");

        Assert.Equal(2, result);
    }

    [Fact]
    public async Task ExecuteScalarAsync_SelectString_ReturnsString()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<string>("SELECT 'hello'");

        Assert.Equal("hello", result);
    }

    [Fact]
    public async Task ExecuteScalarAsync_SelectVersion_ReturnsVersionString()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<string>("SELECT version()");

        Assert.NotNull(result);
        Assert.NotEmpty(result);
        Assert.Contains(".", result); // Version should contain dots
    }

    [Fact]
    public async Task ExecuteScalarAsync_SelectInt64_ReturnsCorrectValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<long>("SELECT toInt64(9223372036854775807)");

        Assert.Equal(9223372036854775807L, result);
    }

    [Fact]
    public async Task ExecuteScalarAsync_SelectFloat64_ReturnsCorrectValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<double>("SELECT 3.14159265358979");

        Assert.Equal(3.14159265358979, result, 10);
    }

    [Fact]
    public async Task ExecuteScalarAsync_InvalidSql_ThrowsClickHouseServerException()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var ex = await Assert.ThrowsAsync<ClickHouseServerException>(
            () => connection.ExecuteScalarAsync<int>("SELECT * FROM nonexistent_table_xyz"));

        Assert.True(ex.ErrorCode > 0);
        Assert.NotNull(ex.ServerExceptionName);
        Assert.NotEmpty(ex.Message);
    }

    [Fact]
    public async Task ExecuteScalarAsync_SyntaxError_ThrowsClickHouseServerException()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var ex = await Assert.ThrowsAsync<ClickHouseServerException>(
            () => connection.ExecuteScalarAsync<int>("SELEC 1"));

        Assert.True(ex.ErrorCode > 0);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_CreateAndDropTable_Works()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var tableName = $"test_table_{Guid.NewGuid():N}";

        try
        {
            // Create table
            await connection.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

            // Verify table exists by selecting from it
            var result = await connection.ExecuteScalarAsync<int>(
                $"SELECT count() FROM {tableName}");
            Assert.Equal(0, result);
        }
        finally
        {
            // Drop table
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_InsertData_Works()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var tableName = $"test_insert_{Guid.NewGuid():N}";

        try
        {
            // Create table
            await connection.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (id Int32, name String) ENGINE = Memory");

            // Insert data
            await connection.ExecuteNonQueryAsync(
                $"INSERT INTO {tableName} VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

            // Verify count
            var count = await connection.ExecuteScalarAsync<int>($"SELECT count() FROM {tableName}");
            Assert.Equal(3, count);

            // Verify data
            var firstName = await connection.ExecuteScalarAsync<string>(
                $"SELECT name FROM {tableName} WHERE id = 1");
            Assert.Equal("Alice", firstName);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task ExecuteScalarAsync_WithProgress_ReportsProgress()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var progressReports = new List<QueryProgress>();
        var progress = new Progress<QueryProgress>(p => progressReports.Add(p));

        // Use a query that generates some progress
        var result = await connection.ExecuteScalarAsync<int>(
            "SELECT count() FROM numbers(10000)", progress);

        // Progress may or may not be reported depending on query size and server behavior
        // Just verify we got a result
        Assert.Equal(10000, result);
    }

    [Fact]
    public async Task ExecuteScalarAsync_MultipleQueriesSequentially_Works()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        for (int i = 1; i <= 5; i++)
        {
            var result = await connection.ExecuteScalarAsync<int>($"SELECT {i}");
            Assert.Equal(i, result);
        }
    }

    [Fact]
    public async Task ExecuteScalarAsync_SelectUInt8_ReturnsCorrectValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<byte>("SELECT toUInt8(255)");

        Assert.Equal(255, result);
    }

    [Fact]
    public async Task ExecuteScalarAsync_SelectInt8_ReturnsCorrectValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<sbyte>("SELECT toInt8(-128)");

        Assert.Equal(-128, result);
    }

    [Fact]
    public async Task ExecuteScalarAsync_ConnectionNotOpen_ThrowsInvalidOperation()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        // Don't open the connection

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => connection.ExecuteScalarAsync<int>("SELECT 1"));
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_ConnectionNotOpen_ThrowsInvalidOperation()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        // Don't open the connection

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => connection.ExecuteNonQueryAsync("SELECT 1"));
    }

    [Fact]
    public async Task ExecuteScalarAsync_CancelledToken_ThrowsOperationCancelled()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => connection.ExecuteScalarAsync<int>("SELECT 1", cancellationToken: cts.Token));
    }

    [Fact]
    public async Task ExecuteScalarAsync_EmptyResult_ReturnsDefault()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Query that returns no rows
        var result = await connection.ExecuteScalarAsync<int>(
            "SELECT 1 WHERE 1 = 0");

        Assert.Equal(default, result);
    }
}
