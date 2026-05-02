using CH.Native.Connection;
using CH.Native.Telemetry;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pre-fix the data source kicked off prewarm with <c>_ = Task.Run(PrewarmAsync)</c>
/// and silently swallowed failures (caught and broke out of the loop). Operators
/// pointing the pool at a misconfigured host saw an under-filled pool but no
/// signal — first real rent eventually surfaced the problem far from the cause.
///
/// This test pins: a prewarm failure is logged at Warning/Error and the captured
/// task is observable via <see cref="ClickHouseDataSource.PrewarmTask"/>.
/// </summary>
[Trait(Categories.Name, Categories.Resilience)]
public class DataSourcePrewarmFailureTests
{
    private readonly ITestOutputHelper _output;

    public DataSourcePrewarmFailureTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task UnreachableHost_PrewarmFailure_IsLogged()
    {
        var sink = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(b =>
        {
            b.AddProvider(sink);
            b.SetMinimumLevel(LogLevel.Trace);
        });

        var settings = ClickHouseConnectionSettings.CreateBuilder()
            // RFC 5737 TEST-NET-1 — guaranteed non-routable.
            .WithHost("192.0.2.1")
            .WithPort(9000)
            .WithCredentials("default", "ignored")
            .WithTelemetry(new TelemetrySettings { LoggerFactory = loggerFactory })
            .Build();

        var options = new ClickHouseDataSourceOptions
        {
            Settings = settings,
            MinPoolSize = 1,
            MaxPoolSize = 2,
            PrewarmOnStart = true,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(2),
        };

        await using var ds = new ClickHouseDataSource(options);

        // Construction must not block on prewarm; the prewarm task runs in the
        // background. Await the captured task so the test has a deterministic
        // join point.
        await ds.PrewarmTask.ConfigureAwait(false);

        // The failure ends up in the sink — either as PrewarmFailed (preferred)
        // or any Warning/Error mentioning prewarm.
        var prewarmEntries = sink.Entries
            .Where(e => e.Level >= LogLevel.Warning &&
                        (e.Message.Contains("prewarm", StringComparison.OrdinalIgnoreCase) ||
                         e.EventId.Name?.Contains("Prewarm", StringComparison.OrdinalIgnoreCase) == true))
            .ToList();

        foreach (var e in prewarmEntries)
            _output.WriteLine($"[{e.Level}] {e.EventId.Name}: {e.Message}");

        Assert.NotEmpty(prewarmEntries);
    }

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        public List<LogEntry> Entries { get; } = new();

        public ILogger CreateLogger(string categoryName) => new CapturingLogger(this, categoryName);

        public void Dispose() { }

        public sealed class LogEntry
        {
            public required LogLevel Level { get; init; }
            public required EventId EventId { get; init; }
            public required string Message { get; init; }
            public Exception? Exception { get; init; }
        }

        private sealed class CapturingLogger : ILogger
        {
            private readonly CapturingLoggerProvider _owner;
            private readonly string _category;

            public CapturingLogger(CapturingLoggerProvider owner, string category)
            {
                _owner = owner;
                _category = category;
            }

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
                Exception? exception, Func<TState, Exception?, string> formatter)
            {
                lock (_owner.Entries)
                {
                    _owner.Entries.Add(new LogEntry
                    {
                        Level = logLevel,
                        EventId = eventId,
                        Message = formatter(state, exception),
                        Exception = exception,
                    });
                }
            }
        }
    }
}
