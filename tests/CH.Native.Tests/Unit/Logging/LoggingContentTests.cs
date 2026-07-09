using CH.Native.Telemetry;
using Xunit;

namespace CH.Native.Tests.Unit.Logging;

/// <summary>
/// Asserts the rendered <b>content</b> of query log messages (row count + elapsed ms in the
/// completion log, the error text in the failure log) — the existing logger tests only pin the
/// EventId/level.
/// </summary>
public class LoggingContentTests
{
    [Fact]
    public void QueryCompleted_MessageContainsRowCountAndElapsedMs()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.QueryCompleted("q-42", rowCount: 42, durationMs: 3.14);

        var entry = Assert.Single(capture.Entries);
        Assert.Contains("42", entry.Message);
        Assert.Contains("3.1", entry.Message); // {DurationMs:F1}
        Assert.Contains("ms", entry.Message);
    }

    [Fact]
    public void QueryFailed_MessageContainsErrorText()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);

        logger.QueryFailed("q-42", "table does not exist");

        var entry = Assert.Single(capture.Entries);
        Assert.Contains("table does not exist", entry.Message);
    }
}
