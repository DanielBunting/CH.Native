using Microsoft.Extensions.Logging;

namespace CH.Native.Telemetry;

/// <summary>
/// Configuration for CH.Native telemetry (tracing, metrics, logging).
/// </summary>
public sealed record TelemetrySettings
{
    /// <summary>
    /// Enable distributed tracing via ActivitySource.
    /// </summary>
    public bool EnableTracing { get; init; } = true;

    /// <summary>
    /// Enable metrics collection via Meter.
    /// </summary>
    public bool EnableMetrics { get; init; } = true;

    /// <summary>
    /// Include SQL statements in traces (will be sanitized to remove literal values).
    /// </summary>
    public bool IncludeSqlInTraces { get; init; } = true;

    /// <summary>
    /// Logger factory for creating ILogger instances.
    /// Pass your application's ILoggerFactory for structured logging integration.
    /// </summary>
    public ILoggerFactory? LoggerFactory { get; init; }

    /// <summary>
    /// Default settings with telemetry enabled but no logging.
    /// </summary>
    public static TelemetrySettings Default { get; } = new();
}
