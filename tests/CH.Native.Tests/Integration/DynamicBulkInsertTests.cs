using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class DynamicBulkInsertTests
{
    private readonly ClickHouseFixture _fixture;

    public DynamicBulkInsertTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task DynamicBulkInsert_ThreeColumnsTwoRows_RoundTrips()
    {
        var tableName = $"test_dynbulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                ts DateTime,
                event_type String,
                payload String
            ) ENGINE = Memory");

        try
        {
            var ts = new DateTime(2026, 5, 7, 12, 0, 0, DateTimeKind.Utc);
            await connection.BulkInsertAsync(
                tableName,
                new[] { "ts", "event_type", "payload" },
                new[]
                {
                    new object?[] { ts, "click", "{\"page\":\"/home\"}" },
                    new object?[] { ts.AddSeconds(1), "view", "{\"page\":\"/about\"}" },
                });

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);

            var types = new List<string>();
            await foreach (var row in connection.QueryAsync($"SELECT event_type FROM {tableName} ORDER BY ts"))
            {
                types.Add(row.GetFieldValue<string>("event_type"));
            }
            Assert.Equal(new[] { "click", "view" }, types);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task DynamicBulkInsert_100Rows_AllInserted()
    {
        var tableName = $"test_dynbulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Int64
            ) ENGINE = Memory");

        try
        {
            var rows = Enumerable.Range(0, 100)
                .Select(i => new object?[] { i, (long)(i * 100) });

            await connection.BulkInsertAsync(tableName, new[] { "Id", "Value" }, rows);

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(100, count);

            var sum = await connection.ExecuteScalarAsync<long>($"SELECT sum(Value) FROM {tableName}");
            Assert.Equal(495_000L, sum);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task DynamicBulkInsert_AsyncEnumerableOverload_RoundTrips()
    {
        var tableName = $"test_dynbulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Int64
            ) ENGINE = Memory");

        try
        {
            await connection.BulkInsertAsync(
                tableName,
                new[] { "Id", "Value" },
                AsyncRows());

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(50, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }

        static async IAsyncEnumerable<object?[]> AsyncRows()
        {
            for (var i = 0; i < 50; i++)
            {
                await Task.Yield();
                yield return new object?[] { i, (long)i };
            }
        }
    }

    [Fact]
    public async Task DynamicBulkInsert_RowArityMismatch_ThrowsAndConnectionRemainsUsable()
    {
        var tableName = $"test_dynbulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            await using (var inserter = connection.CreateBulkInserter(tableName, new[] { "Id", "Name" }))
            {
                await inserter.InitAsync();

                await Assert.ThrowsAsync<ArgumentException>(async () =>
                    await inserter.AddAsync(new object?[] { 1 })); // row arity 1 vs cols 2

                // The connection should be usable for finishing this insert with a valid row.
                await inserter.AddAsync(new object?[] { 1, "alice" });
                await inserter.CompleteAsync();
            }

            // And reusable for subsequent queries.
            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task DynamicBulkInsert_ColumnTypesProvidedAndComplete_SkipsServerProbe()
    {
        var tableName = $"test_dynbulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions
            {
                ColumnTypes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["Id"] = "Int32",
                    ["Name"] = "String",
                },
                UseSchemaCache = false,
            };

            await connection.BulkInsertAsync(
                tableName,
                new[] { "Id", "Name" },
                new[]
                {
                    new object?[] { 1, "alice" },
                    new object?[] { 2, "bob" },
                },
                options);

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task DynamicBulkInsert_ColumnTypesPartialCoverage_ThrowsAtInit()
    {
        var tableName = $"test_dynbulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions
            {
                // Only one of two columns supplied — strict failure.
                ColumnTypes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["Id"] = "Int32",
                },
            };

            await using var inserter = connection.CreateBulkInserter(tableName, new[] { "Id", "Name" }, options);
            await Assert.ThrowsAsync<InvalidOperationException>(() => inserter.InitAsync());
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task DynamicBulkInsert_CompositeTypes_ArrayMapTuple_RoundTrips()
    {
        var tableName = $"test_dynbulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Tags Array(String),
                Pair Tuple(Int32, String)
            ) ENGINE = Memory");

        try
        {
            await connection.BulkInsertAsync(
                tableName,
                new[] { "Id", "Tags", "Pair" },
                new[]
                {
                    new object?[] { 1, new[] { "a", "b" }, ValueTuple.Create(10, "ten") },
                    new object?[] { 2, Array.Empty<string>(), ValueTuple.Create(20, "twenty") },
                });

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }
}
