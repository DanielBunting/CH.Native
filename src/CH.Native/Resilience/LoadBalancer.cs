namespace CH.Native.Resilience;

/// <summary>
/// Distributes requests across multiple ClickHouse servers based on health status.
/// </summary>
public sealed class LoadBalancer
{
    private readonly HealthChecker _healthChecker;
    private readonly LoadBalancingStrategy _strategy;
    private int _roundRobinIndex;

    /// <summary>
    /// Gets the load balancing strategy being used.
    /// </summary>
    public LoadBalancingStrategy Strategy => _strategy;

    /// <summary>
    /// Creates a new load balancer with the specified health checker and strategy.
    /// </summary>
    /// <param name="healthChecker">The health checker monitoring server health.</param>
    /// <param name="strategy">The load balancing strategy to use. Default is RoundRobin.</param>
    public LoadBalancer(HealthChecker healthChecker, LoadBalancingStrategy strategy = LoadBalancingStrategy.RoundRobin)
    {
        _healthChecker = healthChecker ?? throw new ArgumentNullException(nameof(healthChecker));
        _strategy = strategy;
    }

    /// <summary>
    /// Gets the next available server based on the load balancing strategy.
    /// Selection is best-effort: the caller snapshots the healthy-node set and
    /// then picks an index, so a server marked unhealthy in the narrow window
    /// between snapshot and return can still be returned. The caller's connect
    /// path observes the failure on its own (and feeds it back into the
    /// health checker on the next probe), so this is acceptable noise.
    /// </summary>
    /// <returns>The server address to use, or null if no healthy servers are available.</returns>
    public ServerAddress? GetNextServer()
    {
        var healthyNodes = _healthChecker.GetHealthyNodes().ToList();

        if (healthyNodes.Count == 0)
        {
            return null;
        }

        return _strategy switch
        {
            LoadBalancingStrategy.RoundRobin => GetRoundRobin(healthyNodes),
            LoadBalancingStrategy.Random => GetRandom(healthyNodes),
            LoadBalancingStrategy.FirstAvailable => healthyNodes[0].Address,
            _ => throw new InvalidOperationException($"Unknown load balancing strategy: {_strategy}")
        };
    }

    /// <summary>
    /// Marks a server as having failed, immediately taking it out of rotation.
    /// </summary>
    /// <remarks>
    /// Intended for failures observed by in-flight operations. Unlike the background
    /// probe path (which requires three consecutive failures before flipping the
    /// node), one observed failure here is treated as conclusive — the background
    /// health checker will restore the node on the next successful probe.
    /// </remarks>
    /// <param name="address">The address of the server that failed.</param>
    public void MarkServerFailed(ServerAddress address)
    {
        var node = _healthChecker.Nodes.FirstOrDefault(n => n.Address == address);
        node?.MarkUnhealthyImmediate();
    }

    /// <summary>
    /// Marks a server as healthy, immediately updating its health status.
    /// </summary>
    /// <remarks>
    /// Race with <see cref="MarkServerFailed"/> is intentionally last-write-
    /// wins: the connect path observes a real failure and the background probe
    /// observes a real success, so whichever lands second reflects the most
    /// recent ground truth on the next rent. No need for stronger ordering.
    /// </remarks>
    /// <param name="address">The address of the server that succeeded.</param>
    public void MarkServerHealthy(ServerAddress address)
    {
        var node = _healthChecker.Nodes.FirstOrDefault(n => n.Address == address);
        node?.MarkHealthy();
    }

    /// <summary>
    /// Gets the count of currently healthy servers.
    /// </summary>
    public int HealthyServerCount => _healthChecker.HealthyCount;

    /// <summary>
    /// Gets all server nodes regardless of health status.
    /// </summary>
    public IReadOnlyList<ServerNode> AllServers => _healthChecker.Nodes;

    private ServerAddress GetRoundRobin(List<ServerNode> nodes)
    {
        var index = Interlocked.Increment(ref _roundRobinIndex);
        // Use modulo to wrap around, handling potential int overflow gracefully
        var normalizedIndex = ((index % nodes.Count) + nodes.Count) % nodes.Count;
        return nodes[normalizedIndex].Address;
    }

    private ServerAddress GetRandom(List<ServerNode> nodes)
    {
        return nodes[Random.Shared.Next(nodes.Count)].Address;
    }
}
