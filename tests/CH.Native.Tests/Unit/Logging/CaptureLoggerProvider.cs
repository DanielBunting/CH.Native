using Microsoft.Extensions.Logging;

namespace CH.Native.Tests.Unit.Logging;

/// <summary>
/// Minimal in-memory ILoggerProvider capturing (LogLevel, EventId, rendered message).
/// </summary>
internal sealed class CaptureLoggerProvider : ILoggerProvider, ILoggerFactory
{
    private readonly LogLevel _minLevel;

    public CaptureLoggerProvider(LogLevel minLevel = LogLevel.Trace) => _minLevel = minLevel;

    public List<(LogLevel Level, EventId EventId, string Message)> Entries { get; } = new();

    public ILogger CreateLogger(string categoryName) => new Capture(this, _minLevel);

    public void Dispose() { }

    public void AddProvider(ILoggerProvider provider) { }

    private sealed class Capture : ILogger
    {
        private readonly CaptureLoggerProvider _parent;
        private readonly LogLevel _minLevel;
        public Capture(CaptureLoggerProvider parent, LogLevel minLevel)
        {
            _parent = parent;
            _minLevel = minLevel;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => logLevel >= _minLevel;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel < _minLevel)
                return;
            var msg = formatter(state, exception);
            lock (_parent.Entries)
                _parent.Entries.Add((logLevel, eventId, msg));
        }
    }
}
