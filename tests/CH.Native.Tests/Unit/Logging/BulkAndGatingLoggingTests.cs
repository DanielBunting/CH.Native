using CH.Native.Telemetry;
using Microsoft.Extensions.Logging;
using Xunit;

namespace CH.Native.Tests.Unit.Logging;

/// <summary>
/// Content assertions for the bulk-insert log messages (ported from the driver's
/// ClickHouseBulkCopyLoggingTests — CH.Native emits one schema-resolved line + per-flush + completion,
/// not the driver's start/metadata pairs), and Debug level-gating of query logs (driver's
/// WithoutDebugLogging_DoesNotLogQueryDetails).
/// </summary>
public class BulkAndGatingLoggingTests
{
    [Fact]
    public void BulkInsertSchemaFetched_MessageHasTableAndColumnCount()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.BulkInsertSchemaFetched("users", columnCount: 5, fromCache: true);

        var entry = Assert.Single(capture.Entries);
        Assert.Equal(52, entry.EventId.Id);
        Assert.Contains("users", entry.Message);
        Assert.Contains("5", entry.Message);
    }

    [Fact]
    public void BulkInsertFlushed_MessageHasRowCountAndTable()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.BulkInsertFlushed("users", rowCount: 500);

        var entry = Assert.Single(capture.Entries);
        Assert.Equal(51, entry.EventId.Id);
        Assert.Contains("500", entry.Message);
        Assert.Contains("users", entry.Message);
    }

    [Fact]
    public void BulkInsertCompleted_MessageHasRowsAndElapsedMs()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.BulkInsertCompleted("users", rowCount: 1000, durationMs: 3.14);

        var entry = Assert.Single(capture.Entries);
        Assert.Equal(40, entry.EventId.Id);
        Assert.Contains("users", entry.Message);
        Assert.Contains("1000", entry.Message);
        Assert.Contains("3.1", entry.Message);
        Assert.Contains("ms", entry.Message);
    }

    [Fact]
    public void QueryStarted_BelowDebugLevel_NotLogged()
    {
        var capture = new CaptureLoggerProvider(LogLevel.Information); // Debug disabled
        var logger = new ClickHouseLogger(capture);

        logger.LogQueryStarted("q-id", "SELECT 1");

        Assert.DoesNotContain(capture.Entries, e => e.EventId.Id == 10);
    }

    [Fact]
    public void QueryStarted_AtDebugLevel_Logged()
    {
        var capture = new CaptureLoggerProvider(LogLevel.Debug);
        var logger = new ClickHouseLogger(capture);

        logger.LogQueryStarted("q-id", "SELECT 1");

        Assert.Contains(capture.Entries, e => e.EventId.Id == 10);
    }
}
