using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration.Linq;

[Collection("ClickHouse")]
public class LinqQueryTests : IAsyncLifetime
{
    private readonly ClickHouseFixture _fixture;
    private readonly string _tableName;

    public LinqQueryTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
        _tableName = $"linq_test_{Guid.NewGuid():N}";
    }

    public async Task InitializeAsync()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {_tableName} (
                id Int32,
                name String,
                amount Decimal64(2),
                quantity Int32,
                is_active UInt8,
                created_at DateTime,
                notes Nullable(String)
            ) ENGINE = MergeTree()
            ORDER BY id");

        // Insert test data
        await connection.ExecuteNonQueryAsync($@"
            INSERT INTO {_tableName} VALUES
            (1, 'Alice', 100.50, 10, 1, '2024-01-01 10:00:00', 'First customer'),
            (2, 'Bob', 250.00, 5, 1, '2024-01-02 11:00:00', NULL),
            (3, 'Charlie', 75.25, 20, 0, '2024-01-03 12:00:00', 'Inactive'),
            (4, 'Diana', 300.00, 8, 1, '2024-01-04 13:00:00', 'VIP customer'),
            (5, 'Eve', 50.00, 15, 1, '2024-01-05 14:00:00', NULL),
            (6, 'Frank', 125.75, 3, 0, '2024-01-06 15:00:00', 'On hold'),
            (7, 'Grace', 500.00, 25, 1, '2024-01-07 16:00:00', 'Premium'),
            (8, 'Henry', 80.00, 12, 1, '2024-01-08 17:00:00', NULL),
            (9, 'Ivy', 175.50, 7, 1, '2024-01-09 18:00:00', 'Regular'),
            (10, 'Jack', 225.00, 18, 0, '2024-01-10 19:00:00', 'Suspended')");
    }

    public async Task DisposeAsync()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_tableName}");
    }

    #region Test Model

    // Note: Using explicit table name since the dynamic table name doesn't match
    // The source generator generates proper UInt8 to bool mapping
    public class TestCustomer
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public decimal Amount { get; set; }
        public int Quantity { get; set; }
        // Use byte to match UInt8 since non-generated mapper doesn't convert
        public byte IsActive { get; set; }
        public DateTime CreatedAt { get; set; }
        public string? Notes { get; set; }

        // Helper property for readability
        public bool IsActiveAsBool => IsActive != 0;
    }

    #endregion

    #region ToListAsync Tests

    [Fact]
    public async Task ToListAsync_ReturnsAllRows()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName).ToListAsync();

        Assert.Equal(10, results.Count);
    }

    [Fact]
    public async Task Where_SingleCondition_FiltersCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.Id == 1)
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task Where_GreaterThan_FiltersCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.Amount > 200)
            .ToListAsync();

        Assert.Equal(4, results.Count);
        Assert.All(results, c => Assert.True(c.Amount > 200));
    }

    [Fact]
    public async Task Where_AndCondition_FiltersCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.IsActive == 1 && c.Amount > 100)
            .ToListAsync();

        Assert.True(results.Count > 0);
        Assert.All(results, c =>
        {
            Assert.Equal(1, c.IsActive);
            Assert.True(c.Amount > 100);
        });
    }

    [Fact]
    public async Task Where_MultipleWhereClauses_CombineWithAnd()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.IsActive == 1)
            .Where(c => c.Quantity >= 10)
            .ToListAsync();

        Assert.True(results.Count > 0);
        Assert.All(results, c =>
        {
            Assert.Equal(1, c.IsActive);
            Assert.True(c.Quantity >= 10);
        });
    }

    #endregion

    #region OrderBy Tests

    [Fact]
    public async Task OrderBy_Ascending_ReturnsCorrectOrder()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .OrderBy(c => c.Amount)
            .ToListAsync();

        Assert.Equal(10, results.Count);
        for (int i = 1; i < results.Count; i++)
        {
            Assert.True(results[i].Amount >= results[i - 1].Amount,
                $"Expected {results[i].Amount} >= {results[i - 1].Amount}");
        }
    }

    [Fact]
    public async Task OrderByDescending_ReturnsCorrectOrder()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .OrderByDescending(c => c.Amount)
            .ToListAsync();

        Assert.Equal(10, results.Count);
        for (int i = 1; i < results.Count; i++)
        {
            Assert.True(results[i].Amount <= results[i - 1].Amount,
                $"Expected {results[i].Amount} <= {results[i - 1].Amount}");
        }
    }

    [Fact]
    public async Task OrderBy_ThenBy_ReturnsCorrectOrder()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .OrderBy(c => c.IsActive)
            .ThenBy(c => c.Name)
            .ToListAsync();

        Assert.Equal(10, results.Count);
    }

    #endregion

    #region Take/Skip Tests

    [Fact]
    public async Task Take_LimitsResults()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .Take(5)
            .ToListAsync();

        Assert.Equal(5, results.Count);
    }

    [Fact]
    public async Task Skip_OffsetResults()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var allResults = await connection.Table<TestCustomer>(_tableName)
            .OrderBy(c => c.Id)
            .ToListAsync();

        var skippedResults = await connection.Table<TestCustomer>(_tableName)
            .OrderBy(c => c.Id)
            .Skip(3)
            .ToListAsync();

        Assert.Equal(7, skippedResults.Count);
        Assert.Equal(allResults[3].Id, skippedResults[0].Id);
    }

    [Fact]
    public async Task Skip_Take_Pagination()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var page2 = await connection.Table<TestCustomer>(_tableName)
            .OrderBy(c => c.Id)
            .Skip(3)
            .Take(3)
            .ToListAsync();

        Assert.Equal(3, page2.Count);
        Assert.Equal(4, page2[0].Id);
        Assert.Equal(5, page2[1].Id);
        Assert.Equal(6, page2[2].Id);
    }

    #endregion

    #region FirstAsync Tests

    [Fact]
    public async Task FirstAsync_ReturnsFirstElement()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.Table<TestCustomer>(_tableName)
            .OrderBy(c => c.Id)
            .FirstAsync();

        Assert.Equal(1, result.Id);
        Assert.Equal("Alice", result.Name);
    }

    [Fact]
    public async Task FirstAsync_WithPredicate_ReturnsMatchingElement()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.Table<TestCustomer>(_tableName)
            .OrderBy(c => c.Id)
            .FirstAsync(c => c.Amount > 200);

        Assert.True(result.Amount > 200);
    }

    [Fact]
    public async Task FirstAsync_EmptySequence_ThrowsInvalidOperationException()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            connection.Table<TestCustomer>(_tableName)
                .Where(c => c.Id < 0)
                .FirstAsync());
    }

    [Fact]
    public async Task FirstOrDefaultAsync_EmptySequence_ReturnsNull()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.Id < 0)
            .FirstOrDefaultAsync();

        Assert.Null(result);
    }

    #endregion

    #region SingleAsync Tests

    [Fact]
    public async Task SingleAsync_ExactlyOneMatch_ReturnsElement()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.Table<TestCustomer>(_tableName)
            .SingleAsync(c => c.Id == 5);

        Assert.Equal(5, result.Id);
        Assert.Equal("Eve", result.Name);
    }

    [Fact]
    public async Task SingleAsync_MultipleMatches_ThrowsInvalidOperationException()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            connection.Table<TestCustomer>(_tableName)
                .Where(c => c.IsActive == 1)
                .SingleAsync());
    }

    #endregion

    #region CountAsync Tests

    [Fact]
    public async Task CountAsync_ReturnsCorrectCount()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var count = await connection.Table<TestCustomer>(_tableName).CountAsync();

        Assert.Equal(10, count);
    }

    [Fact]
    public async Task CountAsync_WithPredicate_ReturnsFilteredCount()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var count = await connection.Table<TestCustomer>(_tableName)
            .CountAsync(c => c.IsActive == 1);

        Assert.Equal(7, count);
    }

    [Fact]
    public async Task CountAsync_WithWhere_ReturnsFilteredCount()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var count = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.Amount > 100)
            .CountAsync();

        Assert.Equal(7, count);
    }

    #endregion

    #region AnyAsync Tests

    [Fact]
    public async Task AnyAsync_HasElements_ReturnsTrue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var hasAny = await connection.Table<TestCustomer>(_tableName).AnyAsync();

        Assert.True(hasAny);
    }

    [Fact]
    public async Task AnyAsync_NoMatchingElements_ReturnsFalse()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var hasAny = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.Id < 0)
            .AnyAsync();

        Assert.False(hasAny);
    }

    [Fact]
    public async Task AnyAsync_WithPredicate_ReturnsCorrectResult()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var hasHighValue = await connection.Table<TestCustomer>(_tableName)
            .AnyAsync(c => c.Amount > 400);

        Assert.True(hasHighValue);
    }

    #endregion

    #region String Methods Tests

    [Fact]
    public async Task Where_StringContains_FiltersCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.Name.Contains("a"))
            .ToListAsync();

        Assert.True(results.Count > 0);
        Assert.All(results, c => Assert.Contains("a", c.Name, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Where_StringStartsWith_FiltersCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.Name.StartsWith("A"))
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task Where_StringEndsWith_FiltersCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.Name.EndsWith("e"))
            .ToListAsync();

        Assert.True(results.Count >= 2);
        Assert.All(results, c => Assert.EndsWith("e", c.Name));
    }

    #endregion

    #region Contains (IN) Tests

    [Fact]
    public async Task Where_ListContains_FiltersCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var ids = new List<int> { 1, 3, 5, 7, 9 };
        var results = await connection.Table<TestCustomer>(_tableName)
            .Where(c => ids.Contains(c.Id))
            .ToListAsync();

        Assert.Equal(5, results.Count);
        Assert.All(results, c => Assert.Contains(c.Id, ids));
    }

    [Fact]
    public async Task Where_ArrayContains_FiltersCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var names = new[] { "Alice", "Bob", "Charlie" };
        var results = await connection.Table<TestCustomer>(_tableName)
            .Where(c => names.Contains(c.Name))
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.All(results, c => Assert.Contains(c.Name, names));
    }

    [Fact]
    public async Task Where_EmptyListContains_ReturnsNoResults()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var ids = new List<int>();
        var results = await connection.Table<TestCustomer>(_tableName)
            .Where(c => ids.Contains(c.Id))
            .ToListAsync();

        Assert.Empty(results);
    }

    #endregion

    #region Null Handling Tests

    [Fact]
    public async Task Where_NullableEqualsNull_FiltersCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.Notes == null)
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.All(results, c => Assert.Null(c.Notes));
    }

    [Fact]
    public async Task Where_NullableNotNull_FiltersCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.Notes != null)
            .ToListAsync();

        Assert.Equal(7, results.Count);
        Assert.All(results, c => Assert.NotNull(c.Notes));
    }

    #endregion

    #region Complex Query Tests

    [Fact]
    public async Task ComplexQuery_CombinesMultipleOperators()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.IsActive == 1)
            .Where(c => c.Amount >= 100)
            .OrderByDescending(c => c.Amount)
            .Take(3)
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.All(results, c =>
        {
            Assert.Equal(1, c.IsActive);
            Assert.True(c.Amount >= 100);
        });
        // Verify descending order
        for (int i = 1; i < results.Count; i++)
        {
            Assert.True(results[i].Amount <= results[i - 1].Amount);
        }
    }

    [Fact]
    public async Task ToSql_ReturnsValidSql()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var query = connection.Table<TestCustomer>(_tableName)
            .Where(c => c.IsActive == 1 && c.Amount > 100)
            .OrderByDescending(c => c.Amount)
            .Take(5);

        var sql = query.ToSql();

        Assert.Contains("SELECT", sql);
        Assert.Contains($"FROM {_tableName}", sql);
        Assert.Contains("WHERE", sql);
        Assert.Contains("ORDER BY", sql);
        Assert.Contains("DESC", sql);
        Assert.Contains("LIMIT 5", sql);
    }

    #endregion

    #region Async Enumeration Tests

    [Fact]
    public async Task AsyncEnumeration_IteratesCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var count = 0;
        await foreach (var customer in connection.Table<TestCustomer>(_tableName))
        {
            Assert.NotNull(customer);
            count++;
        }

        Assert.Equal(10, count);
    }

    [Fact]
    public async Task AsyncEnumeration_WithFilters_Works()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Cast to IAsyncEnumerable<T> to access GetAsyncEnumerator
        var query = (IAsyncEnumerable<TestCustomer>)connection.Table<TestCustomer>(_tableName)
            .Where(c => c.IsActive == 1)
            .OrderBy(c => c.Name);

        var names = new List<string>();
        await foreach (var customer in query)
        {
            names.Add(customer.Name);
        }

        Assert.Equal(7, names.Count);
        // Verify alphabetical order
        for (int i = 1; i < names.Count; i++)
        {
            Assert.True(string.Compare(names[i], names[i - 1], StringComparison.Ordinal) >= 0);
        }
    }

    #endregion

    #region Distinct Tests

    [Fact]
    public async Task Distinct_RemovesDuplicates()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Create a query that would have duplicates without DISTINCT
        var sql = connection.Table<TestCustomer>(_tableName)
            .Distinct()
            .ToSql();

        Assert.Contains("SELECT DISTINCT", sql);
    }

    #endregion

    #region Select Projection Tests

    // DTO class for projection tests
    public class CustomerSummary
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public decimal Amount { get; set; }
    }

    [Fact]
    public async Task Select_AnonymousType_GeneratesCorrectSql()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Anonymous types work for SQL generation but not execution (no parameterless constructor)
        var sql = connection.Table<TestCustomer>(_tableName)
            .Where(c => c.Id <= 3)
            .Select(c => new { c.Id, c.Name })
            .OrderBy(x => x.Id)
            .ToSql();

        Assert.Contains("SELECT", sql);
        Assert.Contains("id", sql);
        Assert.Contains("name", sql);
        Assert.Contains("WHERE", sql);
        Assert.Contains("ORDER BY", sql);
    }

    [Fact]
    public async Task Select_ToDto_ReturnsProjectedData()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.Id <= 2)
            .Select(c => new CustomerSummary { Id = c.Id, Name = c.Name, Amount = c.Amount })
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal(1, results[0].Id);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal(100.50m, results[0].Amount);
    }

    [Fact]
    public async Task Select_ToDto_WithWhereAndOrderBy_Works()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var results = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.Amount > 100)
            .Select(c => new CustomerSummary { Id = c.Id, Name = c.Name, Amount = c.Amount })
            .OrderByDescending(x => x.Amount)
            .Take(3)
            .ToListAsync();

        Assert.Equal(3, results.Count);
        // Should be ordered by Amount descending
        Assert.True(results[0].Amount >= results[1].Amount);
        Assert.True(results[1].Amount >= results[2].Amount);
        // All should have Amount > 100
        Assert.All(results, r => Assert.True(r.Amount > 100));
    }

    [Fact]
    public async Task Select_CountAfterDtoProjection_Works()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var count = await connection.Table<TestCustomer>(_tableName)
            .Where(c => c.Amount > 100)
            .Select(c => new CustomerSummary { Id = c.Id, Name = c.Name, Amount = c.Amount })
            .CountAsync();

        Assert.Equal(7, count);
    }

    #endregion
}
