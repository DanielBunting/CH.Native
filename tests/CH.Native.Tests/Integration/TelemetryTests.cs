using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Telemetry;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class TelemetryTests : IDisposable
{
    private readonly ClickHouseFixture _fixture;
    private readonly ActivityListener _listener;
    private readonly List<Activity> _activities = new();

    public TelemetryTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ClickHouseActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activity => _activities.Add(activity)
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public void Dispose()
    {
        _listener.Dispose();
        _activities.Clear();
    }

    [Fact]
    public async Task Connect_EmitsActivityWithServerInfo()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var connectActivity = _activities.FirstOrDefault(a => a.OperationName == "clickhouse.connect");
        Assert.NotNull(connectActivity);
        Assert.Equal("clickhouse", connectActivity.GetTagItem("db.system"));
        Assert.NotNull(connectActivity.GetTagItem("server.address"));
        Assert.NotNull(connectActivity.GetTagItem("server.port"));
        Assert.NotNull(connectActivity.GetTagItem("db.clickhouse.server_name"));
        Assert.NotNull(connectActivity.GetTagItem("db.clickhouse.server_version"));
    }

    [Fact]
    public async Task ExecuteScalarAsync_EmitsActivityWithCorrectTags()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        _activities.Clear();

        await connection.ExecuteScalarAsync<int>("SELECT 1");

        var queryActivity = _activities.FirstOrDefault(a => a.OperationName == "clickhouse.query");
        Assert.NotNull(queryActivity);
        Assert.Equal("clickhouse", queryActivity.GetTagItem("db.system"));
        Assert.NotNull(queryActivity.GetTagItem("db.statement"));
        Assert.NotNull(queryActivity.GetTagItem("db.clickhouse.query_id"));
    }

    [Fact]
    public async Task ExecuteScalarAsync_SanitizesSqlInActivity()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        _activities.Clear();

        await connection.ExecuteScalarAsync<string>("SELECT 'secret_value'");

        var queryActivity = _activities.FirstOrDefault(a => a.OperationName == "clickhouse.query");
        Assert.NotNull(queryActivity);

        var dbStatement = queryActivity.GetTagItem("db.statement")?.ToString();
        Assert.Equal("SELECT ?", dbStatement);
        Assert.DoesNotContain("secret_value", dbStatement);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_EmitsActivity()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        _activities.Clear();

        // Create a temporary table
        await connection.ExecuteNonQueryAsync(
            "CREATE TEMPORARY TABLE IF NOT EXISTS test_telemetry (id UInt32)");

        var queryActivity = _activities.FirstOrDefault(a => a.OperationName == "clickhouse.query");
        Assert.NotNull(queryActivity);
        Assert.Equal("clickhouse", queryActivity.GetTagItem("db.system"));
    }

    [Fact]
    public async Task ErrorQuery_SetsActivityStatusToError()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        _activities.Clear();

        await Assert.ThrowsAsync<ClickHouseServerException>(
            () => connection.ExecuteScalarAsync<int>("SELECT invalid_column FROM nonexistent_table"));

        var queryActivity = _activities.FirstOrDefault(a => a.OperationName == "clickhouse.query");
        Assert.NotNull(queryActivity);
        Assert.Equal(ActivityStatusCode.Error, queryActivity.Status);
        Assert.NotNull(queryActivity.GetTagItem("error.type"));
        Assert.NotNull(queryActivity.GetTagItem("error.message"));
        Assert.NotNull(queryActivity.GetTagItem("db.clickhouse.error_code"));
    }

    [Fact]
    public async Task TelemetryDisabled_NoActivitiesEmitted()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithTelemetry(new TelemetrySettings { EnableTracing = false })
            .Build();

        _activities.Clear();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();
        await connection.ExecuteScalarAsync<int>("SELECT 1");

        // No activities should be emitted when tracing is disabled
        Assert.Empty(_activities);
    }

    [Fact]
    public async Task QueryAsync_EmitsActivityForReader()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        _activities.Clear();

        await foreach (var row in connection.QueryAsync("SELECT number FROM system.numbers LIMIT 5"))
        {
            // Just iterate through
        }

        var queryActivity = _activities.FirstOrDefault(a => a.OperationName == "clickhouse.query");
        Assert.NotNull(queryActivity);
        Assert.Equal("clickhouse", queryActivity.GetTagItem("db.system"));
    }
}
