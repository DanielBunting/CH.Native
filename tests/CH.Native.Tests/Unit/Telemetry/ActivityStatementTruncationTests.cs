using System.Diagnostics;
using System.Linq;
using CH.Native.Telemetry;
using Xunit;

namespace CH.Native.Tests.Unit.Telemetry;

/// <summary>
/// Covers <see cref="TelemetrySettings.StatementMaxLength"/> truncation of the db.statement trace tag.
/// </summary>
public class ActivityStatementTruncationTests : IDisposable
{
    private readonly ActivityListener _listener;

    public ActivityStatementTruncationTests()
    {
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ClickHouseActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public void Dispose() => _listener.Dispose();

    private static string LongSql() =>
        "SELECT " + string.Join(", ", Enumerable.Range(0, 100).Select(i => $"col{i}"));

    [Fact]
    public void StatementMaxLength_TruncatesTagWithEllipsis()
    {
        var settings = new TelemetrySettings { StatementMaxLength = 20 };
        using var activity = ClickHouseActivitySource.StartQuery(LongSql(), settings: settings);

        var tag = activity!.GetTagItem("db.statement")?.ToString();
        Assert.NotNull(tag);
        Assert.Equal(21, tag!.Length);      // 20 chars + ellipsis
        Assert.EndsWith("…", tag);
    }

    [Fact]
    public void NoStatementMaxLength_KeepsFullStatement()
    {
        var settings = new TelemetrySettings { StatementMaxLength = null };
        using var activity = ClickHouseActivitySource.StartQuery(LongSql(), settings: settings);

        var tag = activity!.GetTagItem("db.statement")?.ToString();
        Assert.NotNull(tag);
        Assert.True(tag!.Length > 100);
        Assert.DoesNotContain("…", tag);
    }
}
