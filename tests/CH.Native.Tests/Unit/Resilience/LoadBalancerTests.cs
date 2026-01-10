using CH.Native.Connection;
using CH.Native.Resilience;
using Xunit;

namespace CH.Native.Tests.Unit.Resilience;

public class LoadBalancerTests
{
    private static ClickHouseConnectionSettings CreateTestSettings()
    {
        return ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithPort(9000)
            .Build();
    }

    [Fact]
    public async Task GetNextServer_RoundRobin_CyclesThrough()
    {
        var servers = new[]
        {
            new ServerAddress("server1", 9000),
            new ServerAddress("server2", 9000),
            new ServerAddress("server3", 9000)
        };

        await using var healthChecker = CreateTestHealthChecker(servers);
        var balancer = new LoadBalancer(healthChecker, LoadBalancingStrategy.RoundRobin);

        var first = balancer.GetNextServer();
        var second = balancer.GetNextServer();
        var third = balancer.GetNextServer();
        var fourth = balancer.GetNextServer(); // Should wrap around

        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.NotNull(third);
        Assert.NotNull(fourth);

        // Round robin should cycle through servers
        Assert.NotEqual(first, second);
        Assert.NotEqual(second, third);
    }

    [Fact]
    public async Task GetNextServer_FirstAvailable_ReturnsFirstHealthy()
    {
        var servers = new[]
        {
            new ServerAddress("server1", 9000),
            new ServerAddress("server2", 9000),
            new ServerAddress("server3", 9000)
        };

        await using var healthChecker = CreateTestHealthChecker(servers);
        var balancer = new LoadBalancer(healthChecker, LoadBalancingStrategy.FirstAvailable);

        var result1 = balancer.GetNextServer();
        var result2 = balancer.GetNextServer();
        var result3 = balancer.GetNextServer();

        // FirstAvailable should always return the same server
        Assert.NotNull(result1);
        Assert.Equal(result1, result2);
        Assert.Equal(result2, result3);
    }

    [Fact]
    public async Task GetNextServer_Random_ReturnsValidServer()
    {
        var servers = new[]
        {
            new ServerAddress("server1", 9000),
            new ServerAddress("server2", 9000),
            new ServerAddress("server3", 9000)
        };

        await using var healthChecker = CreateTestHealthChecker(servers);
        var balancer = new LoadBalancer(healthChecker, LoadBalancingStrategy.Random);

        // Make multiple calls to ensure we get valid servers
        for (var i = 0; i < 100; i++)
        {
            var result = balancer.GetNextServer();
            Assert.NotNull(result);
            Assert.Contains(result.Value, servers);
        }
    }

    [Fact]
    public async Task GetNextServer_NoHealthyServers_ReturnsNull()
    {
        var servers = new[]
        {
            new ServerAddress("server1", 9000),
            new ServerAddress("server2", 9000)
        };

        await using var healthChecker = CreateTestHealthChecker(servers, allUnhealthy: true);
        var balancer = new LoadBalancer(healthChecker);

        var result = balancer.GetNextServer();
        Assert.Null(result);
    }

    [Fact]
    public async Task MarkServerFailed_UpdatesHealthStatus()
    {
        var servers = new[]
        {
            new ServerAddress("server1", 9000),
            new ServerAddress("server2", 9000)
        };

        await using var healthChecker = CreateTestHealthChecker(servers);
        var balancer = new LoadBalancer(healthChecker);

        Assert.Equal(2, balancer.HealthyServerCount);

        // Mark server1 as failed 3 times (threshold to become unhealthy)
        balancer.MarkServerFailed(servers[0]);
        balancer.MarkServerFailed(servers[0]);
        balancer.MarkServerFailed(servers[0]);

        Assert.Equal(1, balancer.HealthyServerCount);
    }

    [Fact]
    public async Task MarkServerHealthy_UpdatesHealthStatus()
    {
        var servers = new[]
        {
            new ServerAddress("server1", 9000),
            new ServerAddress("server2", 9000)
        };

        await using var healthChecker = CreateTestHealthChecker(servers, allUnhealthy: true);
        var balancer = new LoadBalancer(healthChecker);

        Assert.Equal(0, balancer.HealthyServerCount);

        balancer.MarkServerHealthy(servers[0]);

        Assert.Equal(1, balancer.HealthyServerCount);
    }

    [Fact]
    public async Task GetNextServer_SkipsUnhealthyServers()
    {
        var servers = new[]
        {
            new ServerAddress("server1", 9000),
            new ServerAddress("server2", 9000),
            new ServerAddress("server3", 9000)
        };

        await using var healthChecker = CreateTestHealthChecker(servers);
        var balancer = new LoadBalancer(healthChecker, LoadBalancingStrategy.FirstAvailable);

        // Mark first server as unhealthy
        balancer.MarkServerFailed(servers[0]);
        balancer.MarkServerFailed(servers[0]);
        balancer.MarkServerFailed(servers[0]);

        // Should skip server1 and return server2
        var result = balancer.GetNextServer();
        Assert.NotNull(result);
        Assert.NotEqual(servers[0], result.Value);
    }

    [Fact]
    public async Task Strategy_ReturnsConfiguredStrategy()
    {
        var servers = new[] { new ServerAddress("server1", 9000) };
        await using var healthChecker = CreateTestHealthChecker(servers);

        var roundRobin = new LoadBalancer(healthChecker, LoadBalancingStrategy.RoundRobin);
        var random = new LoadBalancer(healthChecker, LoadBalancingStrategy.Random);
        var first = new LoadBalancer(healthChecker, LoadBalancingStrategy.FirstAvailable);

        Assert.Equal(LoadBalancingStrategy.RoundRobin, roundRobin.Strategy);
        Assert.Equal(LoadBalancingStrategy.Random, random.Strategy);
        Assert.Equal(LoadBalancingStrategy.FirstAvailable, first.Strategy);
    }

    [Fact]
    public async Task AllServers_ReturnsAllNodes()
    {
        var servers = new[]
        {
            new ServerAddress("server1", 9000),
            new ServerAddress("server2", 9000)
        };

        await using var healthChecker = CreateTestHealthChecker(servers);
        var balancer = new LoadBalancer(healthChecker);

        Assert.Equal(2, balancer.AllServers.Count);
        Assert.Contains(balancer.AllServers, n => n.Address == servers[0]);
        Assert.Contains(balancer.AllServers, n => n.Address == servers[1]);
    }

    [Fact]
    public async Task GetNextServer_RoundRobin_IsThreadSafe()
    {
        var servers = new[]
        {
            new ServerAddress("server1", 9000),
            new ServerAddress("server2", 9000),
            new ServerAddress("server3", 9000)
        };

        await using var healthChecker = CreateTestHealthChecker(servers);
        var balancer = new LoadBalancer(healthChecker, LoadBalancingStrategy.RoundRobin);

        var results = new System.Collections.Concurrent.ConcurrentBag<ServerAddress>();
        var tasks = new List<Task>();

        for (var i = 0; i < 10; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (var j = 0; j < 100; j++)
                {
                    var server = balancer.GetNextServer();
                    if (server.HasValue)
                        results.Add(server.Value);
                }
            }));
        }

        await Task.WhenAll(tasks);

        Assert.Equal(1000, results.Count);
        // All results should be from our server list
        Assert.All(results, r => Assert.Contains(r, servers));
    }

    [Fact]
    public void ServerAddress_Parse_ValidFormats()
    {
        var host = ServerAddress.Parse("localhost");
        Assert.Equal("localhost", host.Host);
        Assert.Equal(9000, host.Port);

        var hostPort = ServerAddress.Parse("localhost:9001");
        Assert.Equal("localhost", hostPort.Host);
        Assert.Equal(9001, hostPort.Port);

        var ipv4 = ServerAddress.Parse("192.168.1.1:9000");
        Assert.Equal("192.168.1.1", ipv4.Host);
        Assert.Equal(9000, ipv4.Port);

        var ipv6 = ServerAddress.Parse("[::1]:9000");
        Assert.Equal("::1", ipv6.Host);
        Assert.Equal(9000, ipv6.Port);

        var ipv6NoPort = ServerAddress.Parse("[::1]");
        Assert.Equal("::1", ipv6NoPort.Host);
        Assert.Equal(9000, ipv6NoPort.Port);
    }

    [Fact]
    public void ServerAddress_Parse_InvalidFormats()
    {
        Assert.Throws<ArgumentException>(() => ServerAddress.Parse("localhost:invalid"));
        Assert.Throws<ArgumentException>(() => ServerAddress.Parse("localhost:0"));
        Assert.Throws<ArgumentException>(() => ServerAddress.Parse("localhost:65536"));
        Assert.Throws<ArgumentException>(() => ServerAddress.Parse("[::1"));
    }

    [Fact]
    public void ServerAddress_TryParse_ReturnsCorrectly()
    {
        Assert.True(ServerAddress.TryParse("localhost:9000", out var valid));
        Assert.Equal("localhost", valid.Host);
        Assert.Equal(9000, valid.Port);

        Assert.False(ServerAddress.TryParse("localhost:invalid", out _));
    }

    [Fact]
    public void ServerAddress_ToString_ReturnsHostPort()
    {
        var address = new ServerAddress("localhost", 9001);
        Assert.Equal("localhost:9001", address.ToString());
    }

    /// <summary>
    /// Creates a HealthChecker for testing with a very long interval to prevent background checks.
    /// </summary>
    private static HealthChecker CreateTestHealthChecker(IEnumerable<ServerAddress> servers, bool allUnhealthy = false)
    {
        var healthChecker = new HealthChecker(
            servers,
            CreateTestSettings(),
            TimeSpan.FromHours(1)); // Very long interval to prevent background checks

        if (allUnhealthy)
        {
            foreach (var node in healthChecker.Nodes)
            {
                node.MarkUnhealthy();
                node.MarkUnhealthy();
                node.MarkUnhealthy();
            }
        }

        return healthChecker;
    }
}
