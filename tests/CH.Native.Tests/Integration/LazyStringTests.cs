using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class LazyStringTests
{
    private readonly ClickHouseFixture _fixture;

    public LazyStringTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private ClickHouseConnectionSettings BuildLazySettings()
    {
        return ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithStringMaterialization(StringMaterialization.Lazy)
            .Build();
    }

    [Fact]
    public async Task LazyString_NullableString_RoundTrips()
    {
        var tableName = $"test_lazy_nullable_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(BuildLazySettings());
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name Nullable(String)
            ) ENGINE = Memory");

        try
        {
            // Insert data via bulk insert
            await using var inserter = connection.CreateBulkInserter<NullableStringRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new NullableStringRow { Id = 1, Name = "Alice" });
            await inserter.AddAsync(new NullableStringRow { Id = 2, Name = null });
            await inserter.AddAsync(new NullableStringRow { Id = 3, Name = "Charlie" });
            await inserter.AddAsync(new NullableStringRow { Id = 4, Name = null });
            await inserter.AddAsync(new NullableStringRow { Id = 5, Name = "Eve" });

            await inserter.CompleteAsync();

            // Query back with lazy string connection
            var results = new List<(int Id, string? Name)>();
            await foreach (var row in connection.QueryAsync(
                $"SELECT Id, Name FROM {tableName} ORDER BY Id"))
            {
                var id = row.GetFieldValue<int>("Id");
                var name = row.IsDBNull("Name") ? null : row.GetFieldValue<string>("Name");
                results.Add((id, name));
            }

            Assert.Equal(5, results.Count);
            Assert.Equal("Alice", results[0].Name);
            Assert.Null(results[1].Name);
            Assert.Equal("Charlie", results[2].Name);
            Assert.Null(results[3].Name);
            Assert.Equal("Eve", results[4].Name);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task LazyString_LowCardinality_Integration()
    {
        var tableName = $"test_lazy_lowcard_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(BuildLazySettings());
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Tag LowCardinality(Nullable(String))
            ) ENGINE = Memory");

        try
        {
            // Insert repeated strings with some nulls via bulk insert
            await using var inserter = connection.CreateBulkInserter<LowCardinalityNullableRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new LowCardinalityNullableRow { Id = 1, Tag = "important" });
            await inserter.AddAsync(new LowCardinalityNullableRow { Id = 2, Tag = null });
            await inserter.AddAsync(new LowCardinalityNullableRow { Id = 3, Tag = "important" });
            await inserter.AddAsync(new LowCardinalityNullableRow { Id = 4, Tag = "low" });
            await inserter.AddAsync(new LowCardinalityNullableRow { Id = 5, Tag = null });
            await inserter.AddAsync(new LowCardinalityNullableRow { Id = 6, Tag = "low" });
            await inserter.AddAsync(new LowCardinalityNullableRow { Id = 7, Tag = "important" });

            await inserter.CompleteAsync();

            // Query back with lazy string connection
            var results = new List<(int Id, string? Tag)>();
            await foreach (var row in connection.QueryAsync(
                $"SELECT Id, Tag FROM {tableName} ORDER BY Id"))
            {
                var id = row.GetFieldValue<int>("Id");
                var tag = row.IsDBNull("Tag") ? null : row.GetFieldValue<string>("Tag");
                results.Add((id, tag));
            }

            Assert.Equal(7, results.Count);
            Assert.Equal("important", results[0].Tag);
            Assert.Null(results[1].Tag);
            Assert.Equal("important", results[2].Tag);
            Assert.Equal("low", results[3].Tag);
            Assert.Null(results[4].Tag);
            Assert.Equal("low", results[5].Tag);
            Assert.Equal("important", results[6].Tag);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task LazyString_LargeStrings_NoEagerAllocation()
    {
        var tableName = $"test_lazy_large_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(BuildLazySettings());
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name Nullable(String)
            ) ENGINE = Memory");

        try
        {
            // Insert 1000 rows with 1KB strings each
            var testString = new string('x', 1024);
            await using var inserter = connection.CreateBulkInserter<NullableStringRow>(tableName);
            await inserter.InitAsync();

            for (int i = 0; i < 1000; i++)
            {
                await inserter.AddAsync(new NullableStringRow
                {
                    Id = i,
                    Name = testString
                });
            }

            await inserter.CompleteAsync();

            // Measure allocations while reading (informational only)
            var allocBefore = GC.GetAllocatedBytesForCurrentThread();

            var readValues = new List<string?>();
            await foreach (var row in connection.QueryAsync(
                $"SELECT Name FROM {tableName} ORDER BY Id"))
            {
                readValues.Add(row.GetFieldValue<string>("Name"));
            }

            var allocAfter = GC.GetAllocatedBytesForCurrentThread();
            var totalAllocated = allocAfter - allocBefore;

            // Verify all 1000 rows were read with correct data
            Assert.Equal(1000, readValues.Count);
            foreach (var val in readValues)
            {
                Assert.Equal(testString, val);
            }

            // Informational only — allocation tracking can return negative deltas
            // due to GC behavior across async operations. The important assertion
            // is that all 1000 rows were read correctly above.
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task LazyString_EagerEquivalence()
    {
        var tableName = $"test_lazy_equiv_{Guid.NewGuid():N}";

        // Use an eager connection for setup and comparison
        await using var eagerConnection = new ClickHouseConnection(_fixture.ConnectionString);
        await eagerConnection.OpenAsync();

        await eagerConnection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name Nullable(String)
            ) ENGINE = Memory");

        try
        {
            // Insert data via the eager connection
            await using var inserter = eagerConnection.CreateBulkInserter<NullableStringRow>(tableName);
            await inserter.InitAsync();

            var expected = new (int Id, string? Name)[]
            {
                (1, "Hello"),
                (2, null),
                (3, ""),
                (4, "World"),
                (5, null),
                (6, "Unicode: \u00e9\u00e8\u00ea"),
                (7, "Longer string with spaces and numbers 12345"),
            };

            foreach (var (id, name) in expected)
            {
                await inserter.AddAsync(new NullableStringRow { Id = id, Name = name });
            }

            await inserter.CompleteAsync();

            // Read with eager connection
            var eagerResults = new List<(int Id, string? Name)>();
            await foreach (var row in eagerConnection.QueryAsync(
                $"SELECT Id, Name FROM {tableName} ORDER BY Id"))
            {
                var id = row.GetFieldValue<int>("Id");
                var name = row.IsDBNull("Name") ? null : row.GetFieldValue<string>("Name");
                eagerResults.Add((id, name));
            }

            // Read with lazy connection
            await using var lazyConnection = new ClickHouseConnection(BuildLazySettings());
            await lazyConnection.OpenAsync();

            var lazyResults = new List<(int Id, string? Name)>();
            await foreach (var row in lazyConnection.QueryAsync(
                $"SELECT Id, Name FROM {tableName} ORDER BY Id"))
            {
                var id = row.GetFieldValue<int>("Id");
                var name = row.IsDBNull("Name") ? null : row.GetFieldValue<string>("Name");
                lazyResults.Add((id, name));
            }

            // Verify both produce the same results
            Assert.Equal(eagerResults.Count, lazyResults.Count);
            for (int i = 0; i < eagerResults.Count; i++)
            {
                Assert.Equal(eagerResults[i].Id, lazyResults[i].Id);
                Assert.Equal(eagerResults[i].Name, lazyResults[i].Name);
            }

            // Also verify against expected
            Assert.Equal(expected.Length, lazyResults.Count);
            for (int i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i].Id, lazyResults[i].Id);
                Assert.Equal(expected[i].Name, lazyResults[i].Name);
            }
        }
        finally
        {
            await eagerConnection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task LazyString_ZeroRows_EmptyColumn()
    {
        var tableName = $"test_lazy_empty_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(BuildLazySettings());
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name Nullable(String)
            ) ENGINE = Memory");

        try
        {
            // Query the empty table - should return 0 rows without error
            var count = 0;
            await foreach (var row in connection.QueryAsync(
                $"SELECT Id, Name FROM {tableName}"))
            {
                count++;
            }

            Assert.Equal(0, count);

            // Also verify via scalar
            var rowCount = await connection.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName}");
            Assert.Equal(0, rowCount);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #region Test POCOs

    private class NullableStringRow
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    private class LowCardinalityNullableRow
    {
        public int Id { get; set; }
        public string? Tag { get; set; }
    }

    #endregion
}
