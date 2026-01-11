using System.Diagnostics;
using CH.Native.Exceptions;
using CH.Native.Protocol.Messages;
using CH.Native.Telemetry;
using Xunit;

namespace CH.Native.Tests.Unit.Telemetry;

public class ActivitySourceTests : IDisposable
{
    private readonly ActivityListener _listener;
    private readonly List<Activity> _activities = new();

    public ActivitySourceTests()
    {
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ClickHouseActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity => _activities.Add(activity)
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public void Dispose()
    {
        _listener.Dispose();
    }

    [Fact]
    public void StartQuery_CreatesActivityWithCorrectName()
    {
        using var activity = ClickHouseActivitySource.StartQuery("SELECT 1");

        Assert.NotNull(activity);
        Assert.Equal("clickhouse.query", activity.OperationName);
        Assert.Equal(ActivityKind.Client, activity.Kind);
    }

    [Fact]
    public void StartQuery_SetsDbSystemTag()
    {
        using var activity = ClickHouseActivitySource.StartQuery("SELECT 1");

        Assert.Equal("clickhouse", activity!.GetTagItem("db.system"));
    }

    [Fact]
    public void StartQuery_SanitizesSqlInDbStatementTag()
    {
        using var activity = ClickHouseActivitySource.StartQuery("SELECT 'secret_value'");

        var dbStatement = activity!.GetTagItem("db.statement")?.ToString();
        Assert.Equal("SELECT ?", dbStatement);
        Assert.DoesNotContain("secret_value", dbStatement);
    }

    [Fact]
    public void StartQuery_SetsQueryIdTag()
    {
        using var activity = ClickHouseActivitySource.StartQuery("SELECT 1", queryId: "test-query-id");

        Assert.Equal("test-query-id", activity!.GetTagItem("db.clickhouse.query_id"));
    }

    [Fact]
    public void StartQuery_SetsDatabaseTag()
    {
        using var activity = ClickHouseActivitySource.StartQuery("SELECT 1", database: "mydb");

        Assert.Equal("mydb", activity!.GetTagItem("db.name"));
    }

    [Fact]
    public void StartQuery_WhenTracingDisabled_ReturnsNull()
    {
        var settings = new TelemetrySettings { EnableTracing = false };

        var activity = ClickHouseActivitySource.StartQuery("SELECT 1", settings: settings);

        Assert.Null(activity);
    }

    [Fact]
    public void StartQuery_WhenIncludeSqlInTracesDisabled_OmitsSqlStatement()
    {
        var settings = new TelemetrySettings { IncludeSqlInTraces = false };

        using var activity = ClickHouseActivitySource.StartQuery("SELECT 'secret'", settings: settings);

        Assert.NotNull(activity);
        Assert.Null(activity.GetTagItem("db.statement"));
    }

    [Fact]
    public void StartConnect_CreatesActivityWithCorrectTags()
    {
        using var activity = ClickHouseActivitySource.StartConnect("localhost", 9000);

        Assert.NotNull(activity);
        Assert.Equal("clickhouse.connect", activity.OperationName);
        Assert.Equal("clickhouse", activity.GetTagItem("db.system"));
        Assert.Equal("localhost", activity.GetTagItem("server.address"));
        Assert.Equal(9000, activity.GetTagItem("server.port"));
    }

    [Fact]
    public void StartConnect_WhenTracingDisabled_ReturnsNull()
    {
        var settings = new TelemetrySettings { EnableTracing = false };

        var activity = ClickHouseActivitySource.StartConnect("localhost", 9000, settings);

        Assert.Null(activity);
    }

    [Fact]
    public void SetServerInfo_SetsServerTags()
    {
        using var activity = ClickHouseActivitySource.StartConnect("localhost", 9000);
        var serverHello = new ServerHello
        {
            ServerName = "ClickHouse",
            VersionMajor = 24,
            VersionMinor = 1,
            ProtocolRevision = 54467,
            Timezone = "UTC"
        };

        ClickHouseActivitySource.SetServerInfo(activity, serverHello);

        Assert.Equal("ClickHouse", activity!.GetTagItem("db.clickhouse.server_name"));
        Assert.Equal("24.1", activity.GetTagItem("db.clickhouse.server_version"));
        Assert.Equal("UTC", activity.GetTagItem("db.clickhouse.timezone"));
    }

    [Fact]
    public void SetQueryResults_SetsResultTags()
    {
        using var activity = ClickHouseActivitySource.StartQuery("SELECT 1");

        ClickHouseActivitySource.SetQueryResults(activity, 100, 4096);

        Assert.Equal(100L, activity!.GetTagItem("db.clickhouse.rows_read"));
        Assert.Equal(4096L, activity.GetTagItem("db.clickhouse.bytes_read"));
    }

    [Fact]
    public void SetError_SetsStatusAndErrorTags()
    {
        using var activity = ClickHouseActivitySource.StartQuery("SELECT 1");
        var exception = new Exception("Test error");

        ClickHouseActivitySource.SetError(activity, exception);

        Assert.Equal(ActivityStatusCode.Error, activity!.Status);
        Assert.Equal("Test error", activity.StatusDescription);
        Assert.Equal(typeof(Exception).FullName, activity.GetTagItem("error.type"));
        Assert.Equal("Test error", activity.GetTagItem("error.message"));
    }

    [Fact]
    public void SetError_WithClickHouseServerException_SetsErrorCode()
    {
        using var activity = ClickHouseActivitySource.StartQuery("SELECT 1");
        var exception = new ClickHouseServerException(62, "Exception", "Syntax error", "");

        ClickHouseActivitySource.SetError(activity, exception);

        Assert.Equal(ActivityStatusCode.Error, activity!.Status);
        Assert.Equal(62, activity.GetTagItem("db.clickhouse.error_code"));
    }

    [Fact]
    public void SetError_WithNullActivity_DoesNotThrow()
    {
        var exception = new Exception("Test error");

        // Should not throw
        ClickHouseActivitySource.SetError(null, exception);
    }
}
