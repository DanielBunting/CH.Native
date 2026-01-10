using CH.Native.BulkInsert;
using CH.Native.Commands;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Test entity with source-generated mapper.
/// </summary>
[ClickHouseTable(TableName = "test_users")]
public partial class TestUser
{
    [ClickHouseColumn(Name = "id")]
    public int Id { get; set; }

    [ClickHouseColumn(Name = "name")]
    public string Name { get; set; } = "";

    [ClickHouseColumn(Name = "email")]
    public string? Email { get; set; }

    [ClickHouseColumn(Name = "age")]
    public int? Age { get; set; }

    [ClickHouseColumn(Name = "balance")]
    public decimal Balance { get; set; }

    [ClickHouseColumn(Name = "is_active")]
    public bool IsActive { get; set; }
}

/// <summary>
/// Test record with generated mapper.
/// </summary>
[ClickHouseTable]
public partial record TestProduct
{
    public int Id { get; init; }
    public string ProductName { get; init; } = "";
    public decimal Price { get; init; }

    [ClickHouseIgnore]
    public string? ComputedField { get; init; }
}

[Collection("ClickHouse")]
public class GeneratedMapperTests
{
    private readonly ClickHouseFixture _fixture;

    public GeneratedMapperTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public void GeneratedMapper_TestUser_HasExpectedTableName()
    {
        Assert.Equal("test_users", TestUser.ClickHouseMapper.TableName);
    }

    [Fact]
    public void GeneratedMapper_TestUser_HasExpectedColumnNames()
    {
        var expected = new[] { "id", "name", "email", "age", "balance", "is_active" };
        Assert.Equal(expected, TestUser.ClickHouseMapper.ColumnNames);
    }

    [Fact]
    public void GeneratedMapper_TestUser_HasExpectedColumnTypes()
    {
        var types = TestUser.ClickHouseMapper.ColumnTypes;
        Assert.Contains("Int32", types);
        Assert.Contains("String", types);
        Assert.Contains("Nullable(String)", types);
        Assert.Contains("Nullable(Int32)", types);
        Assert.Contains("Decimal128(18)", types);
        Assert.Contains("Bool", types);
    }

    [Fact]
    public void GeneratedMapper_TestProduct_UsesTypeNameAsTableName()
    {
        Assert.Equal("TestProduct", TestProduct.ClickHouseMapper.TableName);
    }

    [Fact]
    public void GeneratedMapper_TestProduct_ExcludesIgnoredProperty()
    {
        Assert.DoesNotContain("ComputedField", TestProduct.ClickHouseMapper.ColumnNames);
    }

    [Fact]
    public async Task QueryAsync_WithGeneratedMapper_ReadsDataCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Query using inline data with explicit types matching CLR types
        var command = new ClickHouseCommand(connection,
            @"SELECT
                toInt32(42) AS id,
                'John Doe' AS name,
                'john@example.com' AS email,
                toInt32(30) AS age,
                toDecimal128(1234.56, 18) AS balance,
                true AS is_active");

        var users = new List<TestUser>();
        await foreach (var user in command.QueryAsync<TestUser>())
        {
            users.Add(user);
        }

        Assert.Single(users);
        var result = users[0];
        Assert.Equal(42, result.Id);
        Assert.Equal("John Doe", result.Name);
        Assert.Equal("john@example.com", result.Email);
        Assert.Equal(30, result.Age);
        // Use approximate comparison for decimal due to precision differences
        Assert.True(Math.Abs(result.Balance - 1234.56m) < 0.01m);
        Assert.True(result.IsActive);
    }

    [Fact]
    public async Task QueryAsync_WithGeneratedMapper_HandlesNullableCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Create a temp table to properly test nullable types
        var tableName = $"test_nullable_{Guid.NewGuid():N}";
        await connection.ExecuteNonQueryAsync(
            $@"CREATE TABLE {tableName} (
                id Int32,
                name String,
                email Nullable(String),
                age Nullable(Int32),
                balance Decimal128(18),
                is_active Bool
            ) ENGINE = Memory");

        try
        {
            await connection.ExecuteNonQueryAsync(
                $@"INSERT INTO {tableName} VALUES (1, 'Test', NULL, NULL, 0, false)");

            var command = new ClickHouseCommand(connection,
                $@"SELECT id, name, email, age, balance, is_active FROM {tableName}");

            var users = new List<TestUser>();
            await foreach (var user in command.QueryAsync<TestUser>())
            {
                users.Add(user);
            }

            Assert.Single(users);
            var result = users[0];
            Assert.Equal(1, result.Id);
            Assert.Equal("Test", result.Name);
            Assert.Null(result.Email);
            Assert.Null(result.Age);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task QueryAsync_WithGeneratedMapper_ReadsMultipleRows()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Use explicit casts for proper type matching
        var command = new ClickHouseCommand(connection,
            @"SELECT
                toInt32(number) AS id,
                concat('User ', toString(number)) AS name,
                if(number % 2 = 0, concat('user', toString(number), '@test.com'), NULL)::Nullable(String) AS email,
                if(number % 3 = 0, toInt32(number * 10), NULL)::Nullable(Int32) AS age,
                toDecimal128(number * 100.5, 18) AS balance,
                number % 2 = 0 AS is_active
            FROM numbers(10)");

        var users = new List<TestUser>();
        await foreach (var user in command.QueryAsync<TestUser>())
        {
            users.Add(user);
        }

        Assert.Equal(10, users.Count);

        // Verify first row
        Assert.Equal(0, users[0].Id);
        Assert.Equal("User 0", users[0].Name);
        Assert.Equal("user0@test.com", users[0].Email);
        Assert.Equal(0, users[0].Age);
        Assert.Equal(0m, users[0].Balance);
        Assert.True(users[0].IsActive);

        // Verify row with nulls
        Assert.Equal(1, users[1].Id);
        Assert.Null(users[1].Email);
        Assert.Null(users[1].Age);
        Assert.False(users[1].IsActive);
    }

    [Fact]
    public async Task QueryAsync_WithRecordType_ReadsDataCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var command = new ClickHouseCommand(connection,
            @"SELECT
                toInt32(1) AS Id,
                'Widget' AS ProductName,
                toDecimal128(99.99, 18) AS Price");

        var products = new List<TestProduct>();
        await foreach (var product in command.QueryAsync<TestProduct>())
        {
            products.Add(product);
        }

        Assert.Single(products);
        Assert.Equal(1, products[0].Id);
        Assert.Equal("Widget", products[0].ProductName);
        // Use approximate comparison for decimal due to precision differences
        Assert.True(Math.Abs(products[0].Price - 99.99m) < 0.01m);
    }

    [Fact]
    public async Task BulkInsert_WithGeneratedMapper_WritesDataCorrectly()
    {
        var tableName = $"test_bulk_insert_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Create table with columns matching TestUser's generated column names
        await connection.ExecuteNonQueryAsync(
            $@"CREATE TABLE {tableName} (
                id Int32,
                name String,
                email Nullable(String),
                age Nullable(Int32),
                balance Decimal128(18),
                is_active Bool
            ) ENGINE = Memory");

        try
        {
            // Insert using BulkInserter with generated mapper
            await using var bulkInserter = new BulkInserter<TestUser>(connection, tableName);
            await bulkInserter.InitAsync();

            var users = new[]
            {
                new TestUser { Id = 1, Name = "Alice", Email = "alice@test.com", Age = 25, Balance = 100.50m, IsActive = true },
                new TestUser { Id = 2, Name = "Bob", Email = null, Age = null, Balance = 200.75m, IsActive = false },
                new TestUser { Id = 3, Name = "Charlie", Email = "charlie@test.com", Age = 35, Balance = 300.00m, IsActive = true }
            };

            await bulkInserter.AddRangeAsync(users);
            await bulkInserter.CompleteAsync();

            // Query back and verify using generated mapper
            var command = new ClickHouseCommand(connection,
                $"SELECT id, name, email, age, balance, is_active FROM {tableName} ORDER BY id");

            var results = new List<TestUser>();
            await foreach (var user in command.QueryAsync<TestUser>())
            {
                results.Add(user);
            }

            Assert.Equal(3, results.Count);

            // Verify Alice
            Assert.Equal(1, results[0].Id);
            Assert.Equal("Alice", results[0].Name);
            Assert.Equal("alice@test.com", results[0].Email);
            Assert.Equal(25, results[0].Age);
            Assert.True(Math.Abs(results[0].Balance - 100.50m) < 0.01m);
            Assert.True(results[0].IsActive);

            // Verify Bob (with nulls)
            Assert.Equal(2, results[1].Id);
            Assert.Equal("Bob", results[1].Name);
            Assert.Null(results[1].Email);
            Assert.Null(results[1].Age);
            Assert.True(Math.Abs(results[1].Balance - 200.75m) < 0.01m);
            Assert.False(results[1].IsActive);

            // Verify Charlie
            Assert.Equal(3, results[2].Id);
            Assert.Equal("Charlie", results[2].Name);
            Assert.Equal("charlie@test.com", results[2].Email);
            Assert.Equal(35, results[2].Age);
            Assert.True(Math.Abs(results[2].Balance - 300.00m) < 0.01m);
            Assert.True(results[2].IsActive);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }
}
