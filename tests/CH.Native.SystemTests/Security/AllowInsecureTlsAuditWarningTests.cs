using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Security;

/// <summary>
/// Pins the defence-in-depth audit warning emitted whenever
/// <c>AllowInsecureTls=true</c> is observed at connection-open time. Surface
/// area §7.2 #13 calls this out as a documented user-trap: the flag silently
/// disables certificate validation, and a startup warning is the cheapest
/// breadcrumb back to the documentation.
///
/// <para>
/// Policy implemented: single-shot per <see cref="ClickHouseConnection"/>.
/// A pool that opens N physical connections will emit N warnings — that is
/// intentional defence-in-depth, not noise: an operator scrolling logs sees
/// every fresh handshake renew the audit trail.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Security)]
public class AllowInsecureTlsAuditWarningTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public AllowInsecureTlsAuditWarningTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task AllowInsecureTlsTrue_OpensConnection_EmitsAuditWarning()
    {
        var capture = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(b => b.AddProvider(capture));

        await using var conn = new ClickHouseConnection(_fx.BuildSettings(b => b
            .WithAllowInsecureTls()
            .WithLoggerFactory(loggerFactory)));
        await conn.OpenAsync();

        var warnings = capture.Records
            .Where(r => r.Level == LogLevel.Warning)
            .Select(r => r.Message)
            .ToArray();

        _output.WriteLine($"Captured warnings ({warnings.Length}):");
        foreach (var w in warnings) _output.WriteLine($"  {w}");

        Assert.Contains(warnings, m =>
            m.Contains("AllowInsecureTls", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AllowInsecureTlsFalse_OpensConnection_NoAuditWarningEmitted()
    {
        var capture = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(b => b.AddProvider(capture));

        await using var conn = new ClickHouseConnection(_fx.BuildSettings(b => b
            .WithLoggerFactory(loggerFactory)));
        await conn.OpenAsync();

        Assert.DoesNotContain(capture.Records, r =>
            r.Level == LogLevel.Warning &&
            r.Message.Contains("AllowInsecureTls", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AllowInsecureTlsTrue_WarningMessage_ContainsHostAndProductionWord()
    {
        var capture = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(b => b.AddProvider(capture));

        await using var conn = new ClickHouseConnection(_fx.BuildSettings(b => b
            .WithAllowInsecureTls()
            .WithLoggerFactory(loggerFactory)));
        await conn.OpenAsync();

        var warning = capture.Records.FirstOrDefault(r =>
            r.Level == LogLevel.Warning &&
            r.Message.Contains("AllowInsecureTls", StringComparison.Ordinal));

        Assert.NotNull(warning);
        Assert.Contains(_fx.Host, warning!.Message);
        // Greppable hint that this is a non-production setting.
        Assert.Contains("production", warning.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AllowInsecureTlsTrue_TwoSeparateConnections_EmitTwoWarnings_OnePerInstance()
    {
        // Confirm the per-connection-instance one-shot policy: each fresh
        // ClickHouseConnection that opens with AllowInsecureTls=true emits
        // the warning exactly once.
        var capture = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(b => b.AddProvider(capture));

        for (int i = 0; i < 2; i++)
        {
            await using var conn = new ClickHouseConnection(_fx.BuildSettings(b => b
                .WithAllowInsecureTls()
                .WithLoggerFactory(loggerFactory)));
            await conn.OpenAsync();
        }

        var warningCount = capture.Records.Count(r =>
            r.Level == LogLevel.Warning &&
            r.Message.Contains("AllowInsecureTls", StringComparison.Ordinal));

        _output.WriteLine($"Warnings across two connections: {warningCount}");
        Assert.Equal(2, warningCount);
    }
}

internal sealed class CapturingLoggerProvider : ILoggerProvider
{
    public List<LogRecord> Records { get; } = new();

    public ILogger CreateLogger(string categoryName) => new CapturingLogger(this, categoryName);

    public void Dispose() { }
}

internal sealed record LogRecord(string Category, LogLevel Level, EventId EventId, string Message);

internal sealed class CapturingLogger : ILogger
{
    private readonly CapturingLoggerProvider _owner;
    private readonly string _category;

    public CapturingLogger(CapturingLoggerProvider owner, string category)
    {
        _owner = owner;
        _category = category;
    }

    public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;
    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        var message = formatter(state, exception);
        lock (_owner.Records)
            _owner.Records.Add(new LogRecord(_category, logLevel, eventId, message));
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();
        public void Dispose() { }
    }
}
