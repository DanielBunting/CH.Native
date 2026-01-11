namespace CH.Native.Resilience;

/// <summary>
/// Strategies for selecting servers during load balancing.
/// </summary>
public enum LoadBalancingStrategy
{
    /// <summary>
    /// Cycles through healthy servers in order.
    /// </summary>
    RoundRobin,

    /// <summary>
    /// Randomly selects a healthy server.
    /// </summary>
    Random,

    /// <summary>
    /// Always selects the first available healthy server.
    /// </summary>
    FirstAvailable
}
