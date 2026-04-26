namespace CH.Native.Resilience;

/// <summary>
/// Represents a server node with health tracking information.
/// </summary>
public sealed class ServerNode
{
    private readonly object _lock = new();

    /// <summary>
    /// Gets the server address.
    /// </summary>
    public ServerAddress Address { get; }

    /// <summary>
    /// Gets or sets whether the server is considered healthy.
    /// </summary>
    public bool IsHealthy
    {
        get { lock (_lock) return _isHealthy; }
        internal set { lock (_lock) _isHealthy = value; }
    }
    private bool _isHealthy = true;

    /// <summary>
    /// Gets or sets the time of the last health check.
    /// </summary>
    public DateTime LastCheck
    {
        get { lock (_lock) return _lastCheck; }
        internal set { lock (_lock) _lastCheck = value; }
    }
    private DateTime _lastCheck;

    /// <summary>
    /// Gets or sets the number of consecutive health check failures.
    /// </summary>
    public int ConsecutiveFailures
    {
        get { lock (_lock) return _consecutiveFailures; }
        internal set { lock (_lock) _consecutiveFailures = value; }
    }
    private int _consecutiveFailures;

    /// <summary>
    /// Creates a new server node for the specified address.
    /// </summary>
    /// <param name="address">The server address.</param>
    public ServerNode(ServerAddress address)
    {
        Address = address;
    }

    /// <summary>
    /// Marks the server as healthy, resetting the failure count.
    /// </summary>
    public void MarkHealthy()
    {
        lock (_lock)
        {
            _isHealthy = true;
            _lastCheck = DateTime.UtcNow;
            _consecutiveFailures = 0;
        }
    }

    /// <summary>
    /// Marks the server as having experienced a failure.
    /// After 3 consecutive failures, the server is marked as unhealthy.
    /// </summary>
    /// <remarks>
    /// Intended for background probes, where a single transient blip should not
    /// flip the node out of rotation. Caller-observed failures (an actual query
    /// that just failed against this server) carry stronger signal — those go
    /// through <see cref="MarkUnhealthyImmediate"/> instead.
    /// </remarks>
    public void MarkUnhealthy()
    {
        lock (_lock)
        {
            _consecutiveFailures++;
            _lastCheck = DateTime.UtcNow;
            if (_consecutiveFailures >= 3)
            {
                _isHealthy = false;
            }
        }
    }

    /// <summary>
    /// Marks the server as unhealthy immediately, on the first failure.
    /// </summary>
    /// <remarks>
    /// Used when the failure is observed by an in-flight operation (not a probe).
    /// One real failed query is enough evidence to take the node out of rotation;
    /// the background health checker will restore it on the next successful probe.
    /// </remarks>
    public void MarkUnhealthyImmediate()
    {
        lock (_lock)
        {
            _consecutiveFailures++;
            _lastCheck = DateTime.UtcNow;
            _isHealthy = false;
        }
    }

    /// <summary>
    /// Returns the server address as a string.
    /// </summary>
    public override string ToString() => Address.ToString();
}
