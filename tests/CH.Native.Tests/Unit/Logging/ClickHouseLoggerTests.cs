using CH.Native.Telemetry;
using Microsoft.Extensions.Logging;
using Xunit;

namespace CH.Native.Tests.Unit.Logging;

public class ClickHouseLoggerTests
{
    [Fact]
    public void WithNullFactory_IsNotEnabled()
    {
        var logger = new ClickHouseLogger(loggerFactory: null);
        Assert.False(logger.IsEnabled);
    }

    [Fact]
    public void ConnectionOpened_FiresEventId1AtInformation()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.ConnectionOpened("localhost", 9000, 12.5, 54467);

        var entry = Assert.Single(capture.Entries);
        Assert.Equal(LogLevel.Information, entry.Level);
        Assert.Equal(1, entry.EventId.Id);
    }

    [Fact]
    public void QueryStarted_FiresEventId10AtDebug()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.QueryStarted("q-id", "SELECT 1");

        var entry = Assert.Single(capture.Entries);
        Assert.Equal(LogLevel.Debug, entry.Level);
        Assert.Equal(10, entry.EventId.Id);
    }

    [Fact]
    public void QueryCompleted_FiresEventId11AtInformation()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.QueryCompleted("q-id", 42, 3.14);

        var entry = Assert.Single(capture.Entries);
        Assert.Equal(LogLevel.Information, entry.Level);
        Assert.Equal(11, entry.EventId.Id);
    }

    [Fact]
    public void QueryFailed_FiresEventId12AtError()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.QueryFailed("q-id", "boom");

        var entry = Assert.Single(capture.Entries);
        Assert.Equal(LogLevel.Error, entry.Level);
        Assert.Equal(12, entry.EventId.Id);
    }

    [Fact]
    public void RetryAttempt_FiresEventId20AtWarning()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.RetryAttempt(2, 3, 500, "timeout");

        var entry = Assert.Single(capture.Entries);
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.Equal(20, entry.EventId.Id);
    }

    [Fact]
    public void CircuitBreakerClosed_FiresEventId22AtInformation()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.CircuitBreakerClosed("host:9000");

        var entry = Assert.Single(capture.Entries);
        Assert.Equal(LogLevel.Information, entry.Level);
        Assert.Equal(22, entry.EventId.Id);
    }

    [Fact]
    public void HealthCheckFailed_FiresEventId23AtWarning()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.HealthCheckFailed("host:9000", "connection refused");

        var entry = Assert.Single(capture.Entries);
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.Equal(23, entry.EventId.Id);
    }

    [Fact]
    public void HealthCheckRecovered_FiresEventId24AtInformation()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.HealthCheckRecovered("host:9000");

        var entry = Assert.Single(capture.Entries);
        Assert.Equal(LogLevel.Information, entry.Level);
        Assert.Equal(24, entry.EventId.Id);
    }

    [Fact]
    public void HandshakeStart_FiresEventId50AtTrace()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.HandshakeStart("localhost", 9000);

        var entry = Assert.Single(capture.Entries);
        Assert.Equal(LogLevel.Trace, entry.Level);
        Assert.Equal(50, entry.EventId.Id);
    }

    [Fact]
    public void BulkInsertFlushed_FiresEventId51AtDebug()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.BulkInsertFlushed("users", 1000);

        var entry = Assert.Single(capture.Entries);
        Assert.Equal(LogLevel.Debug, entry.Level);
        Assert.Equal(51, entry.EventId.Id);
    }

    [Fact]
    public void BulkInsertSchemaFetched_FiresEventId52AtDebug()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.BulkInsertSchemaFetched("users", 5, fromCache: true);

        var entry = Assert.Single(capture.Entries);
        Assert.Equal(LogLevel.Debug, entry.Level);
        Assert.Equal(52, entry.EventId.Id);
    }

    [Fact]
    public void LogQueryStarted_SanitizesLiteralsByDefault()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.LogQueryStarted("q-id", "SELECT * FROM users WHERE password = 'secret-p@ss'");

        var entry = Assert.Single(capture.Entries);
        Assert.DoesNotContain("secret-p@ss", entry.Message);
    }

    [Fact]
    public void LogQueryStarted_TruncatesAt200Chars()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);
        var longSql = "SELECT " + new string('x', 500) + " FROM t";

        logger.LogQueryStarted("q-id", longSql, sanitize: false);

        var entry = Assert.Single(capture.Entries);
        Assert.Contains("...", entry.Message);
    }
}
