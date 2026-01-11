using CH.Native.Connection;
using CH.Native.Results;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class DataReaderTests
{
    private readonly ClickHouseFixture _fixture;

    public DataReaderTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    #region ExecuteReaderAsync Tests

    [Fact]
    public async Task ExecuteReaderAsync_SelectMultipleColumns_ReturnsCorrectMetadata()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync(
            "SELECT 1 as id, 'hello' as name, now() as created");

        // Read to initialize
        Assert.True(await reader.ReadAsync());

        Assert.Equal(3, reader.FieldCount);
        Assert.Equal("id", reader.GetName(0));
        Assert.Equal("name", reader.GetName(1));
        Assert.Equal("created", reader.GetName(2));
    }

    [Fact]
    public async Task ExecuteReaderAsync_SelectMultipleRows_IteratesAllRows()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync(
            "SELECT number FROM numbers(10)");

        var values = new List<ulong>();
        while (await reader.ReadAsync())
        {
            values.Add(reader.GetFieldValue<ulong>(0));
        }

        Assert.Equal(10, values.Count);
        Assert.Equal(Enumerable.Range(0, 10).Select(i => (ulong)i), values);
    }

    [Fact]
    public async Task GetFieldValue_ByOrdinalAndName_ReturnsCorrectValues()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync(
            "SELECT 42 as value, 'test' as text");

        Assert.True(await reader.ReadAsync());

        // By ordinal
        Assert.Equal(42, reader.GetFieldValue<int>(0));
        Assert.Equal("test", reader.GetFieldValue<string>(1));

        // By name
        Assert.Equal(42, reader.GetFieldValue<int>("value"));
        Assert.Equal("test", reader.GetFieldValue<string>("text"));
    }

    [Fact]
    public async Task GetOrdinal_CaseInsensitive_FindsColumn()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync(
            "SELECT 1 as MyColumn");

        Assert.True(await reader.ReadAsync());

        // All case variations should work
        Assert.Equal(0, reader.GetOrdinal("MyColumn"));
        Assert.Equal(0, reader.GetOrdinal("mycolumn"));
        Assert.Equal(0, reader.GetOrdinal("MYCOLUMN"));
        Assert.Equal(0, reader.GetOrdinal("mYcOlUmN"));
    }

    [Fact]
    public async Task ReadAsync_AfterLastRow_ReturnsFalse()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync("SELECT 1");

        Assert.True(await reader.ReadAsync());
        Assert.False(await reader.ReadAsync());
        Assert.False(await reader.ReadAsync()); // Should keep returning false
    }

    [Fact]
    public async Task HasRows_EmptyResult_ReturnsFalse()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync(
            "SELECT 1 WHERE 1 = 0");

        // Need to call ReadAsync to initialize
        Assert.False(await reader.ReadAsync());

        // After initialization, HasRows should be false
        Assert.False(reader.HasRows);
    }

    [Fact]
    public async Task HasRows_WithData_ReturnsTrue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync("SELECT 1");

        Assert.True(await reader.ReadAsync());
        Assert.True(reader.HasRows);
    }

    [Fact]
    public async Task Columns_ReturnsAllMetadata()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync(
            "SELECT toInt32(1) as id, 'hello' as name");

        Assert.True(await reader.ReadAsync());

        var columns = reader.Columns;
        Assert.Equal(2, columns.Count);

        Assert.Equal(0, columns[0].Ordinal);
        Assert.Equal("id", columns[0].Name);
        Assert.Equal("Int32", columns[0].ClickHouseTypeName);

        Assert.Equal(1, columns[1].Ordinal);
        Assert.Equal("name", columns[1].Name);
        Assert.Equal("String", columns[1].ClickHouseTypeName);
    }

    [Fact]
    public async Task IsDBNull_WithNullValue_ReturnsTrue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Use CAST to get a proper Nullable(Int32) type with NULL value
        await using var reader = await connection.ExecuteReaderAsync(
            "SELECT CAST(NULL AS Nullable(Int32)) as nullable_col");

        Assert.True(await reader.ReadAsync());
        Assert.True(reader.IsDBNull(0));
    }

    [Fact]
    public async Task IsDBNull_WithNonNullValue_ReturnsFalse()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync(
            "SELECT toNullable(toInt32(42)) as nullable_col");

        Assert.True(await reader.ReadAsync());
        Assert.False(reader.IsDBNull(0));
    }

    #endregion

    #region Large Result Streaming Tests

    [Fact]
    public async Task ExecuteReaderAsync_LargeResult_IteratesAllRows()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync(
            "SELECT number, toString(number) as str FROM numbers(100000)");

        long count = 0;
        while (await reader.ReadAsync())
        {
            // Verify values are consistent
            var num = reader.GetFieldValue<ulong>(0);
            var str = reader.GetFieldValue<string>(1);
            Assert.Equal(num.ToString(), str);
            count++;
        }

        Assert.Equal(100000, count);
    }

    #endregion

    #region Early Disposal Tests

    [Fact]
    public async Task ExecuteReaderAsync_EarlyDispose_ConnectionStillUsable()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Start reading but don't finish
        await using (var reader = await connection.ExecuteReaderAsync(
            "SELECT number FROM numbers(10000)"))
        {
            // Read just a few rows
            for (int i = 0; i < 10; i++)
            {
                Assert.True(await reader.ReadAsync());
            }
            // Dispose without reading all rows
        }

        // Connection should still be usable
        var result = await connection.ExecuteScalarAsync<int>("SELECT 42");
        Assert.Equal(42, result);
    }

    #endregion

    #region QueryAsync (Row Enumeration) Tests

    [Fact]
    public async Task QueryAsync_EnumeratesRows()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var rows = new List<(int id, string name)>();

        await foreach (var row in connection.QueryAsync(
            "SELECT number as id, toString(number) as name FROM numbers(5)"))
        {
            rows.Add((
                (int)row.GetFieldValue<ulong>("id"),
                row.GetFieldValue<string>("name")
            ));
        }

        Assert.Equal(5, rows.Count);
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(i, rows[i].id);
            Assert.Equal(i.ToString(), rows[i].name);
        }
    }

    [Fact]
    public async Task QueryAsync_EarlyBreak_ConnectionStillUsable()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        int count = 0;
        await foreach (var row in connection.QueryAsync(
            "SELECT number FROM numbers(10000)"))
        {
            count++;
            if (count >= 10) break;
        }

        Assert.Equal(10, count);

        // Connection should still be usable
        var result = await connection.ExecuteScalarAsync<int>("SELECT 42");
        Assert.Equal(42, result);
    }

    [Fact]
    public async Task QueryAsync_EmptyResult_YieldsNothing()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var count = 0;
        await foreach (var row in connection.QueryAsync("SELECT 1 WHERE 1 = 0"))
        {
            count++;
        }

        Assert.Equal(0, count);
    }

    [Fact]
    public async Task QueryAsync_AccessByIndex_Works()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await foreach (var row in connection.QueryAsync("SELECT 42 as value, 'test' as text"))
        {
            // Access by index
            Assert.Equal((byte)42, row[0]);
            Assert.Equal("test", row[1]);

            // FieldCount
            Assert.Equal(2, row.FieldCount);
        }
    }

    #endregion

    #region QueryAsync<T> (Object Mapping) Tests

    public class SimpleDto
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Fact]
    public async Task QueryAsyncT_MapsToDto()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var items = new List<SimpleDto>();
        await foreach (var item in connection.QueryAsync<SimpleDto>(
            "SELECT toInt32(number) as Id, toString(number) as Name FROM numbers(3)"))
        {
            items.Add(item);
        }

        Assert.Equal(3, items.Count);
        Assert.Equal(0, items[0].Id);
        Assert.Equal("0", items[0].Name);
        Assert.Equal(1, items[1].Id);
        Assert.Equal("1", items[1].Name);
        Assert.Equal(2, items[2].Id);
        Assert.Equal("2", items[2].Name);
    }

    public class PartialDto
    {
        public int Id { get; set; }
        public string? ExtraProperty { get; set; }
    }

    [Fact]
    public async Task QueryAsyncT_MissingColumn_LeavesDefault()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var items = new List<PartialDto>();
        await foreach (var item in connection.QueryAsync<PartialDto>(
            "SELECT toInt32(1) as Id"))
        {
            items.Add(item);
        }

        Assert.Single(items);
        Assert.Equal(1, items[0].Id);
        Assert.Null(items[0].ExtraProperty); // Not in query, should be default
    }

    [Fact]
    public async Task QueryAsyncT_ExtraColumn_Ignored()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var items = new List<SimpleDto>();
        // Query has extra column not in DTO
        await foreach (var item in connection.QueryAsync<SimpleDto>(
            "SELECT toInt32(1) as Id, 'test' as Name, now() as ExtraColumn"))
        {
            items.Add(item);
        }

        Assert.Single(items);
        Assert.Equal(1, items[0].Id);
        Assert.Equal("test", items[0].Name);
    }

    public class NullableDto
    {
        public int? NullableId { get; set; }
        public string? NullableName { get; set; }
    }

    [Fact]
    public async Task QueryAsyncT_NullableProperties_HandledCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var items = new List<NullableDto>();
        // Use CAST to get proper Nullable types
        await foreach (var item in connection.QueryAsync<NullableDto>(
            "SELECT CAST(NULL AS Nullable(Int32)) as NullableId, CAST('hello' AS Nullable(String)) as NullableName"))
        {
            items.Add(item);
        }

        Assert.Single(items);
        Assert.Null(items[0].NullableId);
        Assert.Equal("hello", items[0].NullableName);
    }

    [Fact]
    public async Task QueryAsyncT_EmptyResult_YieldsNothing()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var count = 0;
        await foreach (var item in connection.QueryAsync<SimpleDto>(
            "SELECT toInt32(1) as Id, 'test' as Name WHERE 1 = 0"))
        {
            count++;
        }

        Assert.Equal(0, count);
    }

    [Fact]
    public async Task QueryAsyncT_CaseInsensitiveMapping()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var items = new List<SimpleDto>();
        // Column names differ in case from property names
        await foreach (var item in connection.QueryAsync<SimpleDto>(
            "SELECT toInt32(42) as ID, 'test' as NAME"))
        {
            items.Add(item);
        }

        Assert.Single(items);
        Assert.Equal(42, items[0].Id);
        Assert.Equal("test", items[0].Name);
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public async Task GetOrdinal_InvalidColumn_ThrowsArgumentException()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync("SELECT 1 as col");
        await reader.ReadAsync();

        Assert.Throws<ArgumentException>(() => reader.GetOrdinal("nonexistent"));
    }

    [Fact]
    public async Task GetFieldValue_InvalidOrdinal_ThrowsArgumentOutOfRange()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync("SELECT 1 as col");
        await reader.ReadAsync();

        Assert.Throws<ArgumentOutOfRangeException>(() => reader.GetFieldValue<int>(99));
        Assert.Throws<ArgumentOutOfRangeException>(() => reader.GetFieldValue<int>(-1));
    }

    [Fact]
    public async Task GetValue_BeforeRead_ThrowsInvalidOperation()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync("SELECT 1");

        // Haven't called ReadAsync yet
        Assert.Throws<InvalidOperationException>(() => reader.GetValue(0));
    }

    #endregion
}
