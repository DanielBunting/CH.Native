using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class LinqEdgeCaseTests : IAsyncLifetime
{
    private readonly ClickHouseFixture _fixture;
    private readonly string _tableName = $"test_linq_edge_{Guid.NewGuid():N}";
    private ClickHouseConnection _connection = null!;

    public LinqEdgeCaseTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        _connection = new ClickHouseConnection(_fixture.ConnectionString);
        await _connection.OpenAsync();

        await _connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {_tableName} (
                id Int32,
                name String,
                value Int32,
                is_active UInt8
            ) ENGINE = Memory");

        // Insert 10 rows of test data
        for (int i = 1; i <= 10; i++)
        {
            var name = i switch
            {
                1 => "Alice",
                2 => "Bob",
                3 => "Charlie",
                4 => "Alice_Two",
                5 => "Eve",
                6 => "Frank",
                7 => "Grace",
                8 => "Heidi",
                9 => "Ivan",
                10 => "Judy",
                _ => $"User_{i}"
            };
            var isActive = (byte)(i % 2 == 0 ? 0 : 1); // odd ids are active
            await _connection.ExecuteNonQueryAsync(
                $"INSERT INTO {_tableName} VALUES ({i}, '{name}', {i * 10}, {isActive})");
        }
    }

    public async Task DisposeAsync()
    {
        await _connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_tableName}");
        await _connection.DisposeAsync();
    }

    [Fact]
    public async Task Linq_ClosureCapture_CorrectValue()
    {
        int x = 5;
        var results = await _connection.Table<TestItem>(_tableName)
            .Where(r => r.Id == x)
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(5, results[0].Id);
        Assert.Equal("Eve", results[0].Name);
    }

    [Fact]
    public async Task Linq_StringContains_TranslatesToPositionOrLike()
    {
        var results = await _connection.Table<TestItem>(_tableName)
            .Where(r => r.Name.Contains("ice"))
            .ToListAsync();

        // "Alice" and "Alice_Two" both contain "ice"
        Assert.True(results.Count >= 2, $"Expected at least 2 results containing 'ice', got {results.Count}");
        Assert.All(results, r => Assert.Contains("ice", r.Name));
    }

    [Fact]
    public async Task Linq_StringStartsWith_Works()
    {
        var results = await _connection.Table<TestItem>(_tableName)
            .Where(r => r.Name.StartsWith("A"))
            .ToListAsync();

        // "Alice" and "Alice_Two" start with "A"
        Assert.True(results.Count >= 2, $"Expected at least 2 results starting with 'A', got {results.Count}");
        Assert.All(results, r => Assert.StartsWith("A", r.Name));
    }

    [Fact]
    public async Task Linq_OrderByThenBy_MultipleColumns()
    {
        var results = await _connection.Table<TestItem>(_tableName)
            .OrderBy(r => r.IsActive)
            .ThenByDescending(r => r.Name)
            .ToListAsync();

        Assert.Equal(10, results.Count);

        // Verify that IsActive=0 rows come before IsActive=1 rows
        var firstActiveIndex = results.FindIndex(r => r.IsActive == 1);
        var lastInactiveIndex = results.FindLastIndex(r => r.IsActive == 0);

        if (firstActiveIndex >= 0 && lastInactiveIndex >= 0)
        {
            Assert.True(lastInactiveIndex < firstActiveIndex,
                "All inactive rows should come before active rows");
        }

        // Within each group, names should be in descending order
        var inactiveNames = results.Where(r => r.IsActive == 0).Select(r => r.Name).ToList();
        var activeNames = results.Where(r => r.IsActive == 1).Select(r => r.Name).ToList();

        for (int i = 1; i < inactiveNames.Count; i++)
        {
            Assert.True(
                string.Compare(inactiveNames[i - 1], inactiveNames[i], StringComparison.Ordinal) >= 0,
                $"Inactive names should be in descending order: '{inactiveNames[i - 1]}' should come before '{inactiveNames[i]}'");
        }

        for (int i = 1; i < activeNames.Count; i++)
        {
            Assert.True(
                string.Compare(activeNames[i - 1], activeNames[i], StringComparison.Ordinal) >= 0,
                $"Active names should be in descending order: '{activeNames[i - 1]}' should come before '{activeNames[i]}'");
        }
    }

    [Fact]
    public async Task Linq_Skip_Take_Pagination()
    {
        var results = await _connection.Table<TestItem>(_tableName)
            .OrderBy(r => r.Id)
            .Skip(2)
            .Take(3)
            .ToListAsync();

        Assert.Equal(3, results.Count);
        // After ordering by Id and skipping 2, we get Id=3, 4, 5
        Assert.Equal(3, results[0].Id);
        Assert.Equal(4, results[1].Id);
        Assert.Equal(5, results[2].Id);
    }

    [Fact]
    public async Task Linq_Count_TranslatesCorrectly()
    {
        // Odd ids (1,3,5,7,9) are active (IsActive=1)
        var count = await _connection.Table<TestItem>(_tableName)
            .Where(r => r.IsActive == 1)
            .CountAsync();

        Assert.Equal(5, count);
    }

    [Fact]
    public async Task Linq_EmptyTable_ReturnsEmpty()
    {
        var emptyTableName = $"test_linq_empty_{Guid.NewGuid():N}";

        await _connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {emptyTableName} (
                id Int32,
                name String,
                value Int32,
                is_active UInt8
            ) ENGINE = Memory");

        try
        {
            var results = await _connection.Table<TestItem>(emptyTableName)
                .ToListAsync();

            Assert.Empty(results);
        }
        finally
        {
            await _connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {emptyTableName}");
        }
    }

    [Fact]
    public async Task Linq_SelectProjection_ToDto()
    {
        var results = await _connection.Table<TestItem>(_tableName)
            .Select(r => new ProjectedItem { Id = r.Id, Name = r.Name })
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(10, results.Count);
        Assert.Equal(1, results[0].Id);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal(10, results[9].Id);
        Assert.Equal("Judy", results[9].Name);

        // Verify all items have both Id and Name populated
        Assert.All(results, r =>
        {
            Assert.True(r.Id > 0, "Id should be positive");
            Assert.False(string.IsNullOrEmpty(r.Name), "Name should not be empty");
        });
    }

    #region Model Classes

    private class TestItem
    {
        [ClickHouseColumn(Name = "id")]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "name")]
        public string Name { get; set; } = string.Empty;

        [ClickHouseColumn(Name = "value")]
        public int Value { get; set; }

        [ClickHouseColumn(Name = "is_active")]
        public byte IsActive { get; set; }
    }

    private class ProjectedItem
    {
        [ClickHouseColumn(Name = "id")]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "name")]
        public string Name { get; set; } = string.Empty;
    }

    #endregion
}
