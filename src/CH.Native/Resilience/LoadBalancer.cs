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
    /// Marks a server as having failed, immediately updating its health status.
    /// </summary>
    /// <param name="address">The address of the server that failed.</param>
    public void MarkServerFailed(ServerAddress address)
    {
        var node = _healthChecker.Nodes.FirstOrDefault(n => n.Address == address);
        node?.MarkUnhealthy();
    }

    /// <summary>
    /// Marks a server as healthy, immediately updating its health status.
    /// </summary>
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
