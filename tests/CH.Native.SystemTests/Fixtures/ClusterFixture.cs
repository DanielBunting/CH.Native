using CH.Native.Connection;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Testcontainers.ClickHouse;
using Xunit;

namespace CH.Native.SystemTests.Fixtures;

/// <summary>
/// Two-shard, two-replica ClickHouse cluster backed by a single ClickHouse Keeper node.
/// Designed for system tests; not a production topology (single-keeper means no quorum).
///
/// Network layout (all on a private docker network):
///   keeper      — clickhouse/clickhouse-keeper
///   chs1r1      — shard 1, replica 1
///   chs1r2      — shard 1, replica 2
///   chs2r1      — shard 2, replica 1
///   chs2r2      — shard 2, replica 2
/// </summary>
public class ClusterFixture : IAsyncLifetime
{
    public const string ClusterName = "test_cluster";
    public const string Username = "default";
    public const string Password = "test_password";
    private const string Image = "clickhouse/clickhouse-server:24.8";
    private const string KeeperImage = "clickhouse/clickhouse-keeper:24.8";

    private INetwork _network = null!;
    private IContainer _keeper = null!;
    private readonly Dictionary<string, ClickHouseContainer> _nodes = new();

    public IReadOnlyDictionary<string, NodeEndpoint> Endpoints { get; private set; } =
        new Dictionary<string, NodeEndpoint>();

    public NodeEndpoint Shard1Replica1 => Endpoints["chs1r1"];
    public NodeEndpoint Shard1Replica2 => Endpoints["chs1r2"];
    public NodeEndpoint Shard2Replica1 => Endpoints["chs2r1"];
    public NodeEndpoint Shard2Replica2 => Endpoints["chs2r2"];

    public async Task InitializeAsync()
    {
        _network = new NetworkBuilder()
            .WithName($"chnative-cluster-{Guid.NewGuid():N}")
            .Build();
        await _network.CreateAsync();

        var configRoot = Path.Combine(AppContext.BaseDirectory, "docker", "cluster");

        _keeper = new ContainerBuilder()
            .WithImage(KeeperImage)
            .WithName($"keeper-{Guid.NewGuid():N}")
            .WithHostname("keeper")
            .WithNetwork(_network)
            .WithBindMount(Path.Combine(configRoot, "keeper_config.xml"),
                           "/etc/clickhouse-keeper/keeper_config.xml", AccessMode.ReadOnly)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9181))
            .Build();
        await _keeper.StartAsync();

        var nodes = new (string name, string macrosFile)[]
        {
            ("chs1r1", "macros_shard1_replica1.xml"),
            ("chs1r2", "macros_shard1_replica2.xml"),
            ("chs2r1", "macros_shard2_replica1.xml"),
            ("chs2r2", "macros_shard2_replica2.xml"),
        };

        var endpoints = new Dictionary<string, NodeEndpoint>();
        foreach (var (name, macrosFile) in nodes)
        {
            var container = new ClickHouseBuilder()
                .WithImage(Image)
                .WithName($"{name}-{Guid.NewGuid():N}")
                .WithHostname(name)
                .WithNetwork(_network)
                .WithUsername(Username)
                .WithPassword(Password)
                .WithBindMount(Path.Combine(configRoot, "cluster_remote_servers.xml"),
                               "/etc/clickhouse-server/config.d/cluster_remote_servers.xml",
                               AccessMode.ReadOnly)
                .WithBindMount(Path.Combine(configRoot, macrosFile),
                               "/etc/clickhouse-server/config.d/macros.xml",
                               AccessMode.ReadOnly)
                .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9000))
                .Build();
            await container.StartAsync();
            _nodes[name] = container;

            endpoints[name] = new NodeEndpoint(
                container.Hostname,
                container.GetMappedPublicPort(9000));
        }
        Endpoints = endpoints;

        // The cluster XML uses internal hostnames; from outside the docker network we
        // reach each node via its mapped port. Make sure each is reachable through the
        // CH.Native client before tests run.
        foreach (var endpoint in endpoints.Values)
            await WaitForReachableAsync(endpoint);
    }

    public ClickHouseConnectionSettings BuildSettings(NodeEndpoint endpoint,
        Action<ClickHouseConnectionSettingsBuilder>? configure = null)
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(endpoint.Host)
            .WithPort(endpoint.Port)
            .WithCredentials(Username, Password);
        configure?.Invoke(builder);
        return builder.Build();
    }

    /// <summary>Stops a single replica to simulate a failure. Caller is responsible for restarting.</summary>
    public Task StopAsync(string nodeName) => _nodes[nodeName].StopAsync();

    /// <summary>
    /// Restarts a previously stopped node and refreshes its mapped host port — Testcontainers
    /// may rebind to a different host port after a stop/start cycle. Callers must read
    /// <c>Endpoints[nodeName]</c> (or the typed accessors) again after this returns.
    /// </summary>
    public async Task StartAsync(string nodeName)
    {
        var container = _nodes[nodeName];
        await container.StartAsync();
        var refreshed = new Dictionary<string, NodeEndpoint>(Endpoints)
        {
            [nodeName] = new NodeEndpoint(container.Hostname, container.GetMappedPublicPort(9000))
        };
        Endpoints = refreshed;
        await WaitForReachableAsync(refreshed[nodeName]);
    }

    public async Task DisposeAsync()
    {
        foreach (var node in _nodes.Values)
            await node.DisposeAsync();
        await _keeper.DisposeAsync();
        await _network.DeleteAsync();
    }

    private static async Task WaitForReachableAsync(NodeEndpoint endpoint)
    {
        Exception? last = null;
        for (int i = 0; i < 90; i++)
        {
            try
            {
                var settings = ClickHouseConnectionSettings.CreateBuilder()
                    .WithHost(endpoint.Host)
                    .WithPort(endpoint.Port)
                    .WithCredentials(Username, Password)
                    .Build();
                await using var conn = new ClickHouseConnection(settings);
                await conn.OpenAsync();
                return;
            }
            catch (Exception ex)
            {
                last = ex;
                await Task.Delay(1000);
            }
        }
        throw new InvalidOperationException(
            $"Cluster node {endpoint} did not become reachable after 90s. Last error: {last?.Message}",
            last);
    }
}

public readonly record struct NodeEndpoint(string Host, int Port)
{
    public override string ToString() => $"{Host}:{Port}";
}

[CollectionDefinition("Cluster")]
public class ClusterCollection : ICollectionFixture<ClusterFixture> { }
