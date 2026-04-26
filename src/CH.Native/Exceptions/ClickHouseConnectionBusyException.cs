namespace CH.Native.Exceptions;

/// <summary>
/// Thrown when a caller attempts to start an operation on a <see cref="Connection.ClickHouseConnection"/>
/// that is already executing one. The native protocol multiplexes a single TCP stream, so a single
/// connection cannot support concurrent queries — use a separate connection (or the connection pool)
/// for parallel work.
/// </summary>
/// <remarks>
/// Inherits from <see cref="InvalidOperationException"/> so existing ADO.NET-style
/// <c>catch (InvalidOperationException)</c> handlers continue to work, while giving callers and tests
/// a precise type to assert on.
/// </remarks>
public class ClickHouseConnectionBusyException : InvalidOperationException
{
    /// <summary>
    /// Gets the id of the query that is currently in flight on the connection, or
    /// <c>"&lt;handshake&gt;"</c> if the connection is still mid-<c>OpenAsync</c>.
    /// </summary>
    public string InFlightQueryId { get; }

    /// <summary>
    /// Sentinel id placed into <c>_currentQueryId</c> while <c>OpenAsync</c> is running so that a
    /// concurrent caller fires the busy check rather than racing the handshake bytes on the wire.
    /// </summary>
    public const string HandshakeSentinel = "<handshake>";

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseConnectionBusyException"/> class.
    /// </summary>
    /// <param name="inFlightQueryId">The id of the query currently in flight.</param>
    public ClickHouseConnectionBusyException(string inFlightQueryId)
        : base(BuildMessage(inFlightQueryId))
    {
        InFlightQueryId = inFlightQueryId;
    }

    private static string BuildMessage(string inFlightQueryId)
    {
        return $"Connection is already executing query '{inFlightQueryId}'. ClickHouseConnection does not support concurrent operations — use a separate connection (or the pool) for parallel work.";
    }
}
