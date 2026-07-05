using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// A native connection is an implicit session, so a temporary table created on it persists for
/// subsequent queries on that same connection. (Ported from the driver's SessionConnectionTest,
/// dropping the HTTP session on/off machinery.)
/// </summary>
[Collection("ClickHouse")]
public class TempTableLifetimeTests
{
    private readonly ClickHouseFixture _fixture;

    public TempTableLifetimeTests(ClickHouseFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task TemporaryTable_PersistsAcrossQueriesOnSameConnection()
    {
        var table = $"temp_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($"CREATE TEMPORARY TABLE {table} (x Int32)");
        await connection.ExecuteNonQueryAsync($"INSERT INTO {table} VALUES (1), (2), (3)");

        var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {table}");
        Assert.Equal(3, count);

        var sum = await connection.ExecuteScalarAsync<long>($"SELECT sum(x) FROM {table}");
        Assert.Equal(6, sum);
    }
}
