using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace CH.Native.Telemetry;

/// <summary>
/// High-performance logging for CH.Native operations.
/// </summary>
public sealed partial class ClickHouseLogger
{
    private readonly ILogger _logger;

    /// <summary>
    /// Creates a new ClickHouseLogger from the specified logger factory.
    /// </summary>
    /// <param name="loggerFactory">The logger factory to create the logger from, or null to use a no-op logger.</param>
    public ClickHouseLogger(ILoggerFactory? loggerFactory)
    {
        _logger = loggerFactory?.CreateLogger("CH.Native") ?? NullLogger.Instance;
    }

    /// <summary>
    /// Gets whether logging is enabled (i.e., not using NullLogger).
    /// </summary>
    public bool IsEnabled => _logger is not NullLogger;

    #region Connection Events

    /// <summary>
    /// Logs that a connection was successfully opened.
    /// </summary>
    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Information,
        Message = "Connected to ClickHouse at {Host}:{Port} in {DurationMs:F1}ms (protocol version {ProtocolVersion})")]
    public partial void ConnectionOpened(string host, int port, double durationMs, int protocolVersion);

    /// <summary>
    /// Logs that a connection was closed.
    /// </summary>
    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Debug,
        Message = "Disconnected from ClickHouse at {Host}")]
    public partial void ConnectionClosed(string host);

    /// <summary>
    /// Logs that a connection attempt failed.
    /// </summary>
    [LoggerMessage(
        EventId = 3,
        Level = LogLevel.Warning,
        Message = "Connection failed to {Host}:{Port}: {ErrorMessage}")]
    public partial void ConnectionFailed(string host, int port, string errorMessage);

    #endregion

    #region Query Events

    /// <summary>
    /// Logs that a query is starting.
    /// </summary>
    [LoggerMessage(
        EventId = 10,
        Level = LogLevel.Debug,
        Message = "Executing query {QueryId}: {Sql}")]
    public partial void QueryStarted(string queryId, string sql);

    /// <summary>
    /// Logs that a query completed successfully.
    /// </summary>
    [LoggerMessage(
        EventId = 11,
        Level = LogLevel.Information,
        Message = "Query {QueryId} completed: {RowCount} rows in {DurationMs:F1}ms")]
    public partial void QueryCompleted(string queryId, long rowCount, double durationMs);

    /// <summary>
    /// Logs that a query failed.
    /// </summary>
    [LoggerMessage(
        EventId = 12,
        Level = LogLevel.Error,
        Message = "Query {QueryId} failed: {ErrorMessage}")]
    public partial void QueryFailed(string queryId, string errorMessage);

    /// <summary>
    /// Logs that a query is starting, with optional SQL sanitization and truncation.
    /// </summary>
    /// <param name="queryId">The query ID.</param>
    /// <param name="sql">The SQL statement.</param>
    /// <param name="sanitize">Whether to sanitize the SQL by removing literal values.</param>
    public void LogQueryStarted(string queryId, string sql, bool sanitize = true)
    {
        if (!_logger.IsEnabled(LogLevel.Debug))
            return;

        var logSql = sanitize ? SqlSanitizer.Sanitize(sql) : sql;
        if (logSql.Length > 200)
            logSql = logSql[..200] + "...";

        QueryStarted(queryId, logSql);
    }

    #endregion

    #region Resilience Events

    /// <summary>
    /// Logs a retry attempt.
    /// </summary>
    [LoggerMessage(
        EventId = 20,
        Level = LogLevel.Warning,
        Message = "Retry attempt {AttemptNumber}/{MaxRetries} after {DelayMs:F0}ms due to: {ErrorMessage}")]
    public partial void RetryAttempt(int attemptNumber, int maxRetries, double delayMs, string errorMessage);

    /// <summary>
    /// Logs a circuit breaker state change.
    /// </summary>
    [LoggerMessage(
        EventId = 21,
        Level = LogLevel.Warning,
        Message = "Circuit breaker for {ServerAddress} state changed: {OldState} -> {NewState}")]
    public partial void CircuitBreakerStateChanged(string serverAddress, string oldState, string newState);

    #endregion

    #region Protocol Events

    /// <summary>
    /// Logs a protocol message (at Trace level).
    /// </summary>
    [LoggerMessage(
        EventId = 30,
        Level = LogLevel.Trace,
        Message = "{Direction} message type=0x{MessageType:X2} size={Size}")]
    public partial void ProtocolMessage(string direction, byte messageType, int size);

    #endregion

    #region Bulk Insert Events

    /// <summary>
    /// Logs that a bulk insert completed.
    /// </summary>
    [LoggerMessage(
        EventId = 40,
        Level = LogLevel.Information,
        Message = "Bulk insert to {TableName} completed: {RowCount} rows in {DurationMs:F1}ms")]
    public partial void BulkInsertCompleted(string tableName, long rowCount, double durationMs);

    #endregion
}
