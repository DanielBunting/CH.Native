using CH.Native.Connection;
using CH.Native.Resilience;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Testcontainers.ClickHouse;
using Xunit;

namespace CH.Native.SystemTests.Fixtures;

/// <summary>
/// Two ClickHouse nodes, each fronted by its own Toxiproxy listener — lets us drive
/// resilience features (retry, circuit breaker, load balancer, health checker) under
/// asymmetric chaos: one node healthy, the other broken in a specific way.
/// </summary>
public class MultiToxiproxyFixture : IAsyncLifetime
{
    public const string Username = "default";
    public const string Password = "test_password";

    private const int InternalAdminPort = 8474;
    private const int InternalProxyPortA = 9100;
    private const int InternalProxyPortB = 9101;

    private INetwork _network = null!;
    private ClickHouseContainer _chA = null!;
    private ClickHouseContainer _chB = null!;
    private IContainer _toxiproxy = null!;

    public ToxiproxyClient Client { get; private set; } = null!;
    public string ProxyAName => "ch_a";
    public string ProxyBName => "ch_b";

    public string ProxyHost => _toxiproxy.Hostname;
    public int ProxyAPort => _toxiproxy.GetMappedPublicPort(InternalProxyPortA);
    public int ProxyBPort => _toxiproxy.GetMappedPublicPort(InternalProxyPortB);

    public ServerAddress EndpointA => new(ProxyHost, ProxyAPort);
    public ServerAddress EndpointB => new(ProxyHost, ProxyBPort);

    public async Task InitializeAsync()
    {
        _network = new NetworkBuilder().WithName($"chnative-multichaos-{Guid.NewGuid():N}").Build();
        await _network.CreateAsync();

        _chA = new ClickHouseBuilder()
            .WithImage("clickhouse/clickhouse-server:24.8")
            .WithName($"ch-a-{Guid.NewGuid():N}")
            .WithHostname("ch_a")
            .WithNetwork(_network)
            .WithUsername(Username)
            .WithPassword(Password)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9000))
            .Build();
        _chB = new ClickHouseBuilder()
            .WithImage("clickhouse/clickhouse-server:24.8")
            .WithName($"ch-b-{Guid.NewGuid():N}")
            .WithHostname("ch_b")
            .WithNetwork(_network)
            .WithUsername(Username)
            .WithPassword(Password)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9000))
            .Build();
        await Task.WhenAll(_chA.StartAsync(), _chB.StartAsync());

        _toxiproxy = new ContainerBuilder()
            .WithImage("ghcr.io/shopify/toxiproxy:2.9.0")
            .WithName($"toxi-{Guid.NewGuid():N}")
            .WithNetwork(_network)
            .WithPortBinding(InternalProxyPortA, true)
            .WithPortBinding(InternalProxyPortB, true)
            .WithPortBinding(InternalAdminPort, true)
            .WithCommand("-host", "0.0.0.0", "-port", InternalAdminPort.ToString())
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilMessageIsLogged("Starting Toxiproxy HTTP server"))
            .Build();
        await _toxiproxy.StartAsync();

        Client = new ToxiproxyClient(
            $"http://{_toxiproxy.Hostname}:{_toxiproxy.GetMappedPublicPort(InternalAdminPort)}");

        await Client.EnsureProxyAsync(ProxyAName, $"0.0.0.0:{InternalProxyPortA}", "ch_a:9000");
        await Client.EnsureProxyAsync(ProxyBName, $"0.0.0.0:{InternalProxyPortB}", "ch_b:9000");

        await WaitReachableAsync(EndpointA);
        await WaitReachableAsync(EndpointB);
    }

    public ClickHouseConnectionSettings BuildSettings(IEnumerable<ServerAddress> servers,
        Action<ClickHouseConnectionSettingsBuilder>? configure = null)
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder()
            .WithCredentials(Username, Password)
            .WithServers(servers);
        configure?.Invoke(builder);
        return builder.Build();
    }

    public async Task DisposeAsync()
    {
        if (Client is not null) await Client.DisposeAsync();
        if (_toxiproxy is not null) await _toxiproxy.DisposeAsync();
        if (_chA is not null) await _chA.DisposeAsync();
        if (_chB is not null) await _chB.DisposeAsync();
        if (_network is not null) await _network.DeleteAsync();
    }

    private async Task WaitReachableAsync(ServerAddress endpoint)
    {
        for (int i = 0; i < 30; i++)
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
            catch when (i < 29)
            {
                await Task.Delay(500);
            }
        }
    }
}

[CollectionDefinition("MultiToxiproxy")]
public class MultiToxiproxyCollection : ICollectionFixture<MultiToxiproxyFixture> { }
