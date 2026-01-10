using System.Diagnostics;
using System.Diagnostics.Metrics;
using CH.Native.Resilience;

namespace CH.Native.Telemetry;

/// <summary>
/// Meter for CH.Native metrics collection.
/// </summary>
public static class ClickHouseMeter
{
    /// <summary>
    /// The name of the Meter.
    /// </summary>
    public const string Name = "CH.Native";

    private static readonly Meter Meter = new(Name);

    #region Query Metrics

    /// <summary>
    /// Total number of queries executed.
    /// </summary>
    public static readonly Counter<long> QueriesTotal = Meter.CreateCounter<long>(
        "ch_native_queries_total",
        unit: "{queries}",
        description: "Total number of queries executed");

    /// <summary>
    /// Total rows read from server.
    /// </summary>
    public static readonly Counter<long> RowsReadTotal = Meter.CreateCounter<long>(
        "ch_native_rows_read_total",
        unit: "{rows}",
        description: "Total rows read from server");

    /// <summary>
    /// Total rows written to server.
    /// </summary>
    public static readonly Counter<long> RowsWrittenTotal = Meter.CreateCounter<long>(
        "ch_native_rows_written_total",
        unit: "{rows}",
        description: "Total rows written to server");

    /// <summary>
    /// Total bytes sent to server.
    /// </summary>
    public static readonly Counter<long> BytesSentTotal = Meter.CreateCounter<long>(
        "ch_native_bytes_sent_total",
        unit: "By",
        description: "Total bytes sent to server");

    /// <summary>
    /// Total bytes received from server.
    /// </summary>
    public static readonly Counter<long> BytesReceivedTotal = Meter.CreateCounter<long>(
        "ch_native_bytes_received_total",
        unit: "By",
        description: "Total bytes received from server");

    /// <summary>
    /// Total number of errors.
    /// </summary>
    public static readonly Counter<long> ErrorsTotal = Meter.CreateCounter<long>(
        "ch_native_errors_total",
        unit: "{errors}",
        description: "Total number of errors");

    /// <summary>
    /// Query execution duration histogram.
    /// </summary>
    public static readonly Histogram<double> QueryDuration = Meter.CreateHistogram<double>(
        "ch_native_query_duration_seconds",
        unit: "s",
        description: "Query execution duration");

    /// <summary>
    /// Connection establishment duration histogram.
    /// </summary>
    public static readonly Histogram<double> ConnectDuration = Meter.CreateHistogram<double>(
        "ch_native_connect_duration_seconds",
        unit: "s",
        description: "Connection establishment duration");

    #endregion

    #region Connection Metrics

    private static int _activeConnections;

    /// <summary>
    /// Number of active connections.
    /// </summary>
    public static readonly ObservableGauge<int> ActiveConnections = Meter.CreateObservableGauge(
        "ch_native_connections_active",
        () => _activeConnections,
        unit: "{connections}",
        description: "Number of active connections");

    /// <summary>
    /// Increments the active connection count.
    /// </summary>
    public static void IncrementConnections() => Interlocked.Increment(ref _activeConnections);

    /// <summary>
    /// Decrements the active connection count.
    /// </summary>
    public static void DecrementConnections() => Interlocked.Decrement(ref _activeConnections);

    #endregion

    #region Resilience Metrics

    /// <summary>
    /// Total retry attempts.
    /// </summary>
    public static readonly Counter<long> RetryAttemptsTotal = Meter.CreateCounter<long>(
        "ch_native_retry_attempts_total",
        unit: "{attempts}",
        description: "Total retry attempts");

    /// <summary>
    /// Delay before retry attempt histogram.
    /// </summary>
    public static readonly Histogram<double> RetryDelaySeconds = Meter.CreateHistogram<double>(
        "ch_native_retry_delay_seconds",
        unit: "s",
        description: "Delay before retry attempt");

    /// <summary>
    /// Circuit breaker state transitions.
    /// </summary>
    public static readonly Counter<long> CircuitBreakerStateChanges = Meter.CreateCounter<long>(
        "ch_native_circuit_breaker_state_changes_total",
        unit: "{changes}",
        description: "Circuit breaker state transitions");

    private static readonly Dictionary<string, CircuitBreakerState> CircuitBreakerStates = new();
    private static readonly object CircuitBreakerStatesLock = new();

    /// <summary>
    /// Current circuit breaker state gauge (0=Closed, 1=HalfOpen, 2=Open).
    /// </summary>
    public static readonly ObservableGauge<int> CircuitBreakerStateGauge = Meter.CreateObservableGauge(
        "ch_native_circuit_breaker_state",
        GetCircuitBreakerStates,
        unit: "{state}",
        description: "Current circuit breaker state (0=Closed, 1=HalfOpen, 2=Open)");

    /// <summary>
    /// Sets the current state of a circuit breaker for metrics reporting.
    /// </summary>
    /// <param name="serverAddress">The server address identifier.</param>
    /// <param name="state">The current circuit breaker state.</param>
    public static void SetCircuitBreakerState(string serverAddress, CircuitBreakerState state)
    {
        lock (CircuitBreakerStatesLock)
        {
            CircuitBreakerStates[serverAddress] = state;
        }
    }

    private static IEnumerable<Measurement<int>> GetCircuitBreakerStates()
    {
        lock (CircuitBreakerStatesLock)
        {
            foreach (var kvp in CircuitBreakerStates)
            {
                yield return new Measurement<int>(
                    (int)kvp.Value,
                    new KeyValuePair<string, object?>("server.address", kvp.Key));
            }
        }
    }

    #endregion

    #region Helper Methods

    /// <summary>
    /// Records metrics for a completed query.
    /// </summary>
    /// <param name="database">The database name.</param>
    /// <param name="duration">The query duration.</param>
    /// <param name="rowsRead">The number of rows read.</param>
    /// <param name="success">Whether the query succeeded.</param>
    public static void RecordQuery(
        string? database,
        TimeSpan duration,
        long rowsRead,
        bool success)
    {
        var tags = new TagList
        {
            { "db.name", database ?? "default" },
            { "status", success ? "success" : "error" }
        };

        QueriesTotal.Add(1, tags);
        QueryDuration.Record(duration.TotalSeconds, tags);

        if (rowsRead > 0)
            RowsReadTotal.Add(rowsRead, tags);
    }

    /// <summary>
    /// Records metrics for a retry attempt.
    /// </summary>
    /// <param name="attemptNumber">The attempt number (1-based).</param>
    /// <param name="delay">The delay before this attempt.</param>
    /// <param name="exceptionType">The type of exception that triggered the retry.</param>
    public static void RecordRetry(int attemptNumber, TimeSpan delay, string exceptionType)
    {
        var tags = new TagList
        {
            { "attempt", attemptNumber },
            { "error.type", exceptionType }
        };

        RetryAttemptsTotal.Add(1, tags);
        RetryDelaySeconds.Record(delay.TotalSeconds, tags);
    }

    /// <summary>
    /// Records metrics for a circuit breaker state transition.
    /// </summary>
    /// <param name="serverAddress">The server address.</param>
    /// <param name="fromState">The previous state.</param>
    /// <param name="toState">The new state.</param>
    public static void RecordCircuitBreakerTransition(
        string serverAddress,
        CircuitBreakerState fromState,
        CircuitBreakerState toState)
    {
        var tags = new TagList
        {
            { "server.address", serverAddress },
            { "from_state", fromState.ToString().ToLowerInvariant() },
            { "to_state", toState.ToString().ToLowerInvariant() }
        };

        CircuitBreakerStateChanges.Add(1, tags);
        SetCircuitBreakerState(serverAddress, toState);
    }

    #endregion
}
