using CH.Native.Connection;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Testcontainers.ClickHouse;
using Xunit;

namespace CH.Native.SystemTests.Fixtures;

/// <summary>
/// A single ClickHouse node fronted by a Toxiproxy instance. Tests configure toxics via
/// <see cref="Client"/> against the proxy named <see cref="ProxyName"/>; the CH.Native
/// client connects through <see cref="ProxyHost"/>:<see cref="ProxyPort"/>.
/// </summary>
public class ToxiproxyFixture : IAsyncLifetime
{
    public const string ProxyName = "clickhouse";
    public const string Username = "default";
    public const string Password = "test_password";
    private const int InternalProxyPort = 9100;
    private const int InternalAdminPort = 8474;

    private INetwork _network = null!;
    private ClickHouseContainer _clickhouse = null!;
    private IContainer _toxiproxy = null!;

    public ToxiproxyClient Client { get; private set; } = null!;
    public string ProxyHost => _toxiproxy.Hostname;
    public int ProxyPort => _toxiproxy.GetMappedPublicPort(InternalProxyPort);

    public async Task InitializeAsync()
    {
        _network = new NetworkBuilder()
            .WithName($"chnative-chaos-{Guid.NewGuid():N}")
            .Build();
        await _network.CreateAsync();

        _clickhouse = new ClickHouseBuilder()
            .WithImage("clickhouse/clickhouse-server:24.8")
            .WithName($"clickhouse-{Guid.NewGuid():N}")
            .WithHostname("clickhouse")
            .WithNetwork(_network)
            .WithUsername(Username)
            .WithPassword(Password)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9000))
            .Build();
        await _clickhouse.StartAsync();

        _toxiproxy = new ContainerBuilder()
            .WithImage("ghcr.io/shopify/toxiproxy:2.9.0")
            .WithName($"toxiproxy-{Guid.NewGuid():N}")
            .WithNetwork(_network)
            .WithPortBinding(InternalProxyPort, true)
            .WithPortBinding(InternalAdminPort, true)
            // Toxiproxy needs to bind on 0.0.0.0 to be reachable across the network bridge.
            .WithCommand("-host", "0.0.0.0", "-port", InternalAdminPort.ToString())
            // The image is distroless (no shell), so the default port-probe wait strategy
            // can't exec inside the container. Wait on a stdout marker instead.
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilMessageIsLogged("Starting Toxiproxy HTTP server"))
            .Build();
        await _toxiproxy.StartAsync();

        var adminPort = _toxiproxy.GetMappedPublicPort(InternalAdminPort);
        Client = new ToxiproxyClient($"http://{_toxiproxy.Hostname}:{adminPort}");

        await Client.EnsureProxyAsync(
            name: ProxyName,
            listen: $"0.0.0.0:{InternalProxyPort}",
            upstream: "clickhouse:9000");

        // Wait for CH-through-proxy handshake to succeed before tests run, so the first
        // toxic-bearing test doesn't race a still-booting server.
        for (int i = 0; i < 30; i++)
        {
            try
            {
                await using var conn = new ClickHouseConnection(BuildSettings());
                await conn.OpenAsync();
                return;
            }
            catch when (i < 29)
            {
                await Task.Delay(500);
            }
        }
    }

    /// <summary>
    /// Recreates the proxy from scratch (DELETE + POST). This kicks any stale connections
    /// and wipes server-side toxic goroutine state. Without this between tests, partially-
    /// drained connections from earlier chaos toxics (bandwidth, latency, slow_close) hold
    /// the proxy mutex and the admin API hangs until Go's WriteTimeout cuts in with 503.
    /// </summary>
    public Task ResetProxyAsync() =>
        Client.EnsureProxyAsync(ProxyName, $"0.0.0.0:{InternalProxyPort}", "clickhouse:9000");

    public ClickHouseConnectionSettings BuildSettings(
        Action<ClickHouseConnectionSettingsBuilder>? configure = null)
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(ProxyHost)
            .WithPort(ProxyPort)
            .WithCredentials(Username, Password);
        configure?.Invoke(builder);
        return builder.Build();
    }

    public async Task DisposeAsync()
    {
        if (Client is not null) await Client.DisposeAsync();
        if (_toxiproxy is not null) await _toxiproxy.DisposeAsync();
        if (_clickhouse is not null) await _clickhouse.DisposeAsync();
        if (_network is not null) await _network.DeleteAsync();
    }
}

[CollectionDefinition("Toxiproxy")]
public class ToxiproxyCollection : ICollectionFixture<ToxiproxyFixture> { }
