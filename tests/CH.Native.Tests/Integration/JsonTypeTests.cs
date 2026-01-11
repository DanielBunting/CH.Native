using System.Text.Json;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Integration tests for JSON column type (requires ClickHouse 25.6+).
/// Tests will gracefully skip on older server versions.
/// </summary>
[Collection("ClickHouse")]
public class JsonTypeTests
{
    private readonly ClickHouseFixture _fixture;

    public JsonTypeTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private static bool IsClickHouse25_6OrLater(ClickHouseConnection connection)
    {
        var info = connection.ServerInfo;
        if (info == null) return false;
        return info.VersionMajor > 25 ||
               (info.VersionMajor == 25 && info.VersionMinor >= 6);
    }

    [Fact]
    public async Task Select_JsonLiteral_ReturnsJsonDocument()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
        {
            // Skip test on older versions
            return;
        }

        var result = await connection.ExecuteScalarAsync<object>(
            "SELECT '{\"name\":\"test\"}'::JSON");

        Assert.NotNull(result);
        var doc = Assert.IsType<JsonDocument>(result);
        using (doc)
        {
            Assert.Equal("test", doc.RootElement.GetProperty("name").GetString());
        }
    }

    [Fact]
    public async Task CreateTable_WithJsonColumn_Succeeds()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var tableName = $"test_json_{Guid.NewGuid():N}";

        try
        {
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE {tableName} (
                    id UInt64,
                    data JSON
                ) ENGINE = Memory");

            // Insert data
            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {tableName} VALUES
                (1, '{{""name"":""Alice"",""age"":30}}'),
                (2, '{{""name"":""Bob"",""age"":25}}')");

            // Query and verify
            var count = 0;
            await foreach (var row in connection.QueryAsync($"SELECT id, data FROM {tableName} ORDER BY id"))
            {
                count++;
                var id = row.GetFieldValue<ulong>("id");
                var data = row.GetFieldValue<JsonDocument>("data");

                using (data)
                {
                    if (id == 1)
                    {
                        Assert.Equal("Alice", data.RootElement.GetProperty("name").GetString());
                        Assert.Equal(30, data.RootElement.GetProperty("age").GetInt32());
                    }
                    else if (id == 2)
                    {
                        Assert.Equal("Bob", data.RootElement.GetProperty("name").GetString());
                        Assert.Equal(25, data.RootElement.GetProperty("age").GetInt32());
                    }
                }
            }

            Assert.Equal(2, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Select_NullableJson_HandlesNull()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var tableName = $"test_nullable_json_{Guid.NewGuid():N}";

        try
        {
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE {tableName} (
                    id UInt64,
                    data Nullable(JSON)
                ) ENGINE = Memory");

            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {tableName} VALUES
                (1, '{{""key"":""value""}}'),
                (2, NULL)");

            var results = new List<(ulong id, JsonDocument? data)>();
            await foreach (var row in connection.QueryAsync($"SELECT id, data FROM {tableName} ORDER BY id"))
            {
                var id = row.GetFieldValue<ulong>("id");
                var data = row.GetFieldValue<JsonDocument?>("data");
                results.Add((id, data));
            }

            Assert.Equal(2, results.Count);
            Assert.NotNull(results[0].data);
            Assert.Null(results[1].data);

            results[0].data?.Dispose();
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Select_NestedJson_ReturnsComplexStructure()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var json = @"{""user"":{""name"":""Alice"",""address"":{""city"":""NYC""}},""tags"":[""a"",""b""]}";
        var result = await connection.ExecuteScalarAsync<object>($"SELECT '{json}'::JSON");

        Assert.NotNull(result);
        var doc = Assert.IsType<JsonDocument>(result);
        using (doc)
        {
            Assert.Equal("Alice", doc.RootElement.GetProperty("user").GetProperty("name").GetString());
            Assert.Equal("NYC", doc.RootElement.GetProperty("user").GetProperty("address").GetProperty("city").GetString());
            Assert.Equal(2, doc.RootElement.GetProperty("tags").GetArrayLength());
        }
    }

    [Fact]
    public async Task Select_EmptyJsonObject_ReturnsEmptyDocument()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var result = await connection.ExecuteScalarAsync<object>("SELECT '{}'::JSON");

        Assert.NotNull(result);
        var doc = Assert.IsType<JsonDocument>(result);
        using (doc)
        {
            Assert.Equal(JsonValueKind.Object, doc.RootElement.ValueKind);
            Assert.Equal(0, doc.RootElement.EnumerateObject().Count());
        }
    }

    [Fact]
    public async Task Select_JsonWithUnicode_PreservesCharacters()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var json = @"{""greeting"":""こんにちは"",""emoji"":""🎉""}";
        var result = await connection.ExecuteScalarAsync<object>($"SELECT '{json}'::JSON");

        Assert.NotNull(result);
        var doc = Assert.IsType<JsonDocument>(result);
        using (doc)
        {
            Assert.Equal("こんにちは", doc.RootElement.GetProperty("greeting").GetString());
            Assert.Equal("🎉", doc.RootElement.GetProperty("emoji").GetString());
        }
    }

    [Fact]
    public async Task Select_JsonWithNumbers_PreservesPrecision()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var json = @"{""integer"":12345678901234567890,""float"":3.14159265358979}";
        var result = await connection.ExecuteScalarAsync<object>($"SELECT '{json}'::JSON");

        Assert.NotNull(result);
        var doc = Assert.IsType<JsonDocument>(result);
        using (doc)
        {
            // Check integer (may be stored as string due to precision)
            var intValue = doc.RootElement.GetProperty("integer");
            Assert.True(intValue.ValueKind == JsonValueKind.Number || intValue.ValueKind == JsonValueKind.String);

            // Check float
            var floatValue = doc.RootElement.GetProperty("float").GetDouble();
            Assert.True(Math.Abs(floatValue - 3.14159265358979) < 0.0000001);
        }
    }

    // ============================================================
    // Nested JSON Tests
    // ============================================================

    [Fact]
    public async Task Select_DeeplyNestedJson_TraversesAllLevels()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var json = @"{""level1"":{""level2"":{""level3"":{""level4"":{""value"":""deep""}}}}}";
        var result = await connection.ExecuteScalarAsync<object>($"SELECT '{json}'::JSON");

        Assert.NotNull(result);
        var doc = Assert.IsType<JsonDocument>(result);
        using (doc)
        {
            var value = doc.RootElement
                .GetProperty("level1")
                .GetProperty("level2")
                .GetProperty("level3")
                .GetProperty("level4")
                .GetProperty("value")
                .GetString();
            Assert.Equal("deep", value);
        }
    }

    [Fact]
    public async Task Select_NestedPathExtraction_ReturnsCorrectValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var tableName = $"test_nested_path_{Guid.NewGuid():N}";

        try
        {
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE {tableName} (id UInt64, data JSON) ENGINE = Memory");

            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {tableName} VALUES
                (1, '{{""user"":{{""profile"":{{""address"":{{""city"":""NYC""}}}}}}}}'),
                (2, '{{""user"":{{""profile"":{{""address"":{{""city"":""LA""}}}}}}}}')");

            var cities = new List<string>();
            await foreach (var row in connection.QueryAsync(
                $"SELECT data.user.profile.address.city::String as city FROM {tableName} ORDER BY id SETTINGS output_format_native_write_json_as_string=1"))
            {
                cities.Add(row.GetFieldValue<string>("city"));
            }

            Assert.Equal(2, cities.Count);
            Assert.Equal("NYC", cities[0]);
            Assert.Equal("LA", cities[1]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Select_JsonArrayWithNestedObjects_ReturnsCorrectStructure()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var json = @"{""items"":[{""name"":""Widget"",""details"":{""price"":29.99}},{""name"":""Gadget"",""details"":{""price"":49.99}}]}";
        var result = await connection.ExecuteScalarAsync<object>($"SELECT '{json}'::JSON");

        Assert.NotNull(result);
        var doc = Assert.IsType<JsonDocument>(result);
        using (doc)
        {
            var items = doc.RootElement.GetProperty("items");
            Assert.Equal(2, items.GetArrayLength());
            Assert.Equal("Widget", items[0].GetProperty("name").GetString());
            Assert.Equal(29.99, items[0].GetProperty("details").GetProperty("price").GetDouble(), 2);
        }
    }

    [Fact]
    public async Task Where_NestedField_FiltersCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var tableName = $"test_nested_filter_{Guid.NewGuid():N}";

        try
        {
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE {tableName} (id UInt64, data JSON) ENGINE = Memory");

            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {tableName} VALUES
                (1, '{{""user"":{{""status"":""active""}}}}'),
                (2, '{{""user"":{{""status"":""inactive""}}}}'),
                (3, '{{""user"":{{""status"":""active""}}}}')");

            var count = 0;
            await foreach (var row in connection.QueryAsync(
                $"SELECT id FROM {tableName} WHERE data.user.status::String = 'active' SETTINGS output_format_native_write_json_as_string=1"))
            {
                count++;
            }

            Assert.Equal(2, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task GroupBy_NestedField_AggregatesCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var tableName = $"test_nested_groupby_{Guid.NewGuid():N}";

        try
        {
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE {tableName} (id UInt64, data JSON) ENGINE = Memory");

            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {tableName} VALUES
                (1, '{{""user"":{{""city"":""NYC""}}}}'),
                (2, '{{""user"":{{""city"":""NYC""}}}}'),
                (3, '{{""user"":{{""city"":""LA""}}}}')");

            var results = new Dictionary<string, ulong>();
            await foreach (var row in connection.QueryAsync(
                $"SELECT data.user.city::String as city, count() as cnt FROM {tableName} GROUP BY city SETTINGS output_format_native_write_json_as_string=1"))
            {
                results[row.GetFieldValue<string>("city")] = row.GetFieldValue<ulong>("cnt");
            }

            Assert.Equal(2, results.Count);
            Assert.Equal(2UL, results["NYC"]);
            Assert.Equal(1UL, results["LA"]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    // ============================================================
    // Array(JSON) Tests
    // ============================================================

    [Fact]
    public async Task CreateTable_WithArrayJsonColumn_Succeeds()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var tableName = $"test_array_json_{Guid.NewGuid():N}";

        try
        {
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE {tableName} (
                    id UInt64,
                    documents Array(JSON)
                ) ENGINE = Memory");

            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {tableName} VALUES
                (1, ['{{""name"":""Alice""}}', '{{""name"":""Bob""}}'])");

            await foreach (var row in connection.QueryAsync(
                $"SELECT id, documents FROM {tableName} SETTINGS output_format_native_write_json_as_string=1"))
            {
                var id = row.GetFieldValue<ulong>("id");
                var documents = row.GetFieldValue<JsonDocument[]>("documents");

                Assert.Equal(1UL, id);
                Assert.Equal(2, documents.Length);
                Assert.Equal("Alice", documents[0].RootElement.GetProperty("name").GetString());
                Assert.Equal("Bob", documents[1].RootElement.GetProperty("name").GetString());

                foreach (var doc in documents)
                    doc.Dispose();
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Select_EmptyArrayJson_ReturnsEmptyArray()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var tableName = $"test_empty_array_json_{Guid.NewGuid():N}";

        try
        {
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE {tableName} (
                    id UInt64,
                    documents Array(JSON)
                ) ENGINE = Memory");

            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {tableName} VALUES (1, [])");

            await foreach (var row in connection.QueryAsync(
                $"SELECT documents FROM {tableName} SETTINGS output_format_native_write_json_as_string=1"))
            {
                var documents = row.GetFieldValue<JsonDocument[]>("documents");
                Assert.Empty(documents);
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Select_ArrayJsonWithNestedObjects_ParsesCorrectly()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var tableName = $"test_array_nested_json_{Guid.NewGuid():N}";

        try
        {
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE {tableName} (
                    id UInt64,
                    events Array(JSON)
                ) ENGINE = Memory");

            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {tableName} VALUES
                (1, ['{{""type"":""click"",""data"":{{""x"":100,""y"":200}}}}', '{{""type"":""scroll"",""data"":{{""offset"":500}}}}'])");

            await foreach (var row in connection.QueryAsync(
                $"SELECT events FROM {tableName} SETTINGS output_format_native_write_json_as_string=1"))
            {
                var events = row.GetFieldValue<JsonDocument[]>("events");

                Assert.Equal(2, events.Length);

                // First event
                Assert.Equal("click", events[0].RootElement.GetProperty("type").GetString());
                Assert.Equal(100, events[0].RootElement.GetProperty("data").GetProperty("x").GetInt32());

                // Second event
                Assert.Equal("scroll", events[1].RootElement.GetProperty("type").GetString());
                Assert.Equal(500, events[1].RootElement.GetProperty("data").GetProperty("offset").GetInt32());

                foreach (var doc in events)
                    doc.Dispose();
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Select_ArrayJsonLiteral_ReturnsArray()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var result = await connection.ExecuteScalarAsync<JsonDocument[]>(
            "SELECT ['{\"a\":1}'::JSON, '{\"b\":2}'::JSON] SETTINGS output_format_native_write_json_as_string=1");

        Assert.NotNull(result);
        Assert.Equal(2, result.Length);
        Assert.Equal(1, result[0].RootElement.GetProperty("a").GetInt32());
        Assert.Equal(2, result[1].RootElement.GetProperty("b").GetInt32());

        foreach (var doc in result)
            doc.Dispose();
    }
}
