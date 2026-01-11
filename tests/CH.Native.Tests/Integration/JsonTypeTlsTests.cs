using System.Text.Json;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Integration tests for JSON column type over TLS (requires ClickHouse 25.6+).
/// Tests will gracefully skip on older server versions.
/// </summary>
[Collection("ClickHouseTls")]
public class JsonTypeTlsTests
{
    private readonly ClickHouseTlsFixture _fixture;

    public JsonTypeTlsTests(ClickHouseTlsFixture fixture)
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
    public async Task Select_JsonLiteral_OverTls_ReturnsJsonDocument()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

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
    public async Task CreateTable_WithJsonColumn_OverTls_Succeeds()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var tableName = $"test_json_tls_{Guid.NewGuid():N}";

        try
        {
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE {tableName} (
                    id UInt64,
                    data JSON
                ) ENGINE = Memory");

            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {tableName} VALUES
                (1, '{{""secure"":true}}')");

            await foreach (var row in connection.QueryAsync($"SELECT data FROM {tableName}"))
            {
                var data = row.GetFieldValue<JsonDocument>("data");
                using (data)
                {
                    Assert.True(data.RootElement.GetProperty("secure").GetBoolean());
                }
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Select_NestedJson_OverTls_ReturnsComplexStructure()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var json = @"{""user"":{""name"":""Bob""},""roles"":[""admin"",""user""]}";
        var result = await connection.ExecuteScalarAsync<object>($"SELECT '{json}'::JSON");

        Assert.NotNull(result);
        var doc = Assert.IsType<JsonDocument>(result);
        using (doc)
        {
            Assert.Equal("Bob", doc.RootElement.GetProperty("user").GetProperty("name").GetString());
            Assert.Equal(2, doc.RootElement.GetProperty("roles").GetArrayLength());
            Assert.Equal("admin", doc.RootElement.GetProperty("roles")[0].GetString());
        }
    }

    [Fact]
    public async Task Select_NullableJson_OverTls_HandlesNull()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        if (!IsClickHouse25_6OrLater(connection))
            return;

        var tableName = $"test_nullable_json_tls_{Guid.NewGuid():N}";

        try
        {
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE {tableName} (
                    id UInt64,
                    data Nullable(JSON)
                ) ENGINE = Memory");

            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {tableName} VALUES
                (1, '{{""tls"":true}}'),
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
            using (results[0].data!)
            {
                Assert.True(results[0].data!.RootElement.GetProperty("tls").GetBoolean());
            }
            Assert.Null(results[1].data);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }
}
