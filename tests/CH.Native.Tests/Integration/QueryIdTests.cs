using System.Diagnostics;
using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Telemetry;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class QueryIdTests
{
    private readonly ClickHouseFixture _fixture;

    public QueryIdTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ExecuteScalar_WithExplicitQueryId_AppearsInSystemQueryLog()
    {
        var id = $"ch-native-test-{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        var result = await conn.ExecuteScalarAsync<int>("SELECT 1", queryId: id);
        Assert.Equal(1, result);
        Assert.Equal(id, conn.LastQueryId);

        await conn.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");

        var logged = await conn.ExecuteScalarAsync<long>(
            $"SELECT count() FROM system.query_log WHERE query_id = '{id}'");
        Assert.True(logged > 0, $"Query id {id} did not appear in system.query_log");
    }

    [Fact]
    public async Task ExecuteScalar_WithoutExplicitQueryId_GeneratesGuidAndSurfaces()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        await conn.ExecuteScalarAsync<int>("SELECT 42");
        var generated = conn.LastQueryId;
        Assert.NotNull(generated);
        Assert.True(Guid.TryParse(generated, out _), "CurrentQueryId should be a D-format GUID when not caller-supplied");
    }

    [Fact]
    public async Task OpenTelemetryTag_MatchesWireQueryId()
    {
        var capturedActivities = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == ClickHouseActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = a => capturedActivities.Add(a)
        };
        ActivitySource.AddActivityListener(listener);

        var id = $"ch-native-otel-{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        await conn.ExecuteScalarAsync<int>("SELECT 1", queryId: id);

        var queryActivity = capturedActivities.FirstOrDefault(a => a.OperationName == "clickhouse.query");
        Assert.NotNull(queryActivity);
        var tag = queryActivity.GetTagItem("db.clickhouse.query_id");
        Assert.Equal(id, tag);
    }

    [Fact]
    public async Task BulkInsert_WithExplicitQueryId_AppearsInSystemQueryLog()
    {
        var id = $"ch-native-bulk-{Guid.NewGuid():N}";
        var tableName = $"test_queryid_bulk_{Guid.NewGuid():N}";

        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (id Int32) ENGINE = Memory");

            var options = new BulkInsertOptions { QueryId = id };
            await using (var inserter = new BulkInserter<BulkRow>(conn, tableName, options))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new BulkRow { Id = 1 });
                await inserter.CompleteAsync();
            }

            Assert.Equal(id, conn.LastQueryId);

            await conn.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");

            var logged = await conn.ExecuteScalarAsync<long>(
                $"SELECT count() FROM system.query_log WHERE query_id = '{id}'");
            Assert.True(logged > 0, $"Bulk insert query id {id} did not appear in system.query_log");
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task QueryId_ExceedingMaxLength_ThrowsArgumentException()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        var tooLong = new string('x', 129);
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await conn.ExecuteScalarAsync<int>("SELECT 1", queryId: tooLong));
    }

    private sealed class BulkRow
    {
        [ClickHouseColumn(Name = "id")]
        public int Id { get; set; }
    }
}
