using System.Net;
using System.Net.Sockets;
using CH.Native.Connection;
using DotNet.Testcontainers.Builders;
using Testcontainers.ClickHouse;
using Xunit;

namespace CH.Native.SystemTests.Fixtures;

/// <summary>
/// Single-node ClickHouse fixture with a <em>fixed</em> host-port mapping, so the
/// container can be stopped and restarted without invalidating already-constructed
/// <see cref="ClickHouseDataSource"/> instances. Used only by
/// <c>PoolRestartRecoveryTests</c>; the default <see cref="SingleNodeFixture"/>
/// keeps its random-port allocation to avoid host-side port-collision risk for
/// the bulk of the suite.
///
/// <para>Picks the host port at fixture construction time by probing a slice of
/// the IANA dynamic range (49500–49600) for the first free TCP port. A
/// hard-coded port would collide with other CI agents or dev environments;
/// a probe is collision-safe within the slice and deterministic enough for
/// debugging when something does go wrong.</para>
/// </summary>
public sealed class RestartableSingleNodeFixture : IAsyncLifetime
{
    private const string TestUsername = "default";
    private const string TestPassword = "test_password";
    private const int ProbeRangeStart = 49500;
    private const int ProbeRangeEnd = 49600;

    private readonly int _hostPort;
    private readonly ClickHouseContainer _container;

    public RestartableSingleNodeFixture()
    {
        _hostPort = ProbeFreeHostPort();

        _container = new ClickHouseBuilder()
            .WithImage("clickhouse/clickhouse-server:24.8")
            .WithUsername(TestUsername)
            .WithPassword(TestPassword)
            .WithEnvironment("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
            // Two-arg form: pins host:container mapping. Survives docker stop/start
            // on the same container ID, which is what restart-recovery tests need.
            .WithPortBinding(_hostPort, 9000)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9000))
            .Build();
    }

    public string Host => _container.Hostname;
    public int Port => _hostPort;
    public string Username => TestUsername;
    public string Password => TestPassword;
    public string ConnectionString => $"Host={Host};Port={Port};Username={Username};Password={Password}";

    public ClickHouseConnectionSettings BuildSettings(Action<ClickHouseConnectionSettingsBuilder>? configure = null)
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(Host)
            .WithPort(Port)
            .WithCredentials(Username, Password);
        configure?.Invoke(builder);
        return builder.Build();
    }

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        await WaitForHandshakeAsync();
    }

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    /// <summary>Stops the underlying container. Pair with <see cref="StartContainerAsync"/>.</summary>
    public Task StopContainerAsync() => _container.StopAsync();

    /// <summary>
    /// (Re)starts the container and waits for the ClickHouse server to accept a
    /// fresh handshake. The fixed host-port binding is preserved across stop/start,
    /// so callers' cached <see cref="ClickHouseConnectionSettings"/> remain valid.
    /// </summary>
    public async Task StartContainerAsync()
    {
        await _container.StartAsync();
        await WaitForHandshakeAsync();
    }

    private async Task WaitForHandshakeAsync()
    {
        for (int attempt = 1; attempt <= 20; attempt++)
        {
            try
            {
                await using var connection = new ClickHouseConnection(BuildSettings());
                await connection.OpenAsync();
                return;
            }
            catch when (attempt < 20)
            {
                await Task.Delay(500);
            }
        }
    }

    private static int ProbeFreeHostPort()
    {
        // Try each candidate by binding a TcpListener; the first one that binds
        // cleanly is free *right now*. A small race window between probe and the
        // container's bind is acceptable — Docker will fail container start with
        // a clear error, and the fixture surface stays simple.
        for (int port = ProbeRangeStart; port <= ProbeRangeEnd; port++)
        {
            try
            {
                using var listener = new TcpListener(IPAddress.Loopback, port);
                listener.Start();
                listener.Stop();
                return port;
            }
            catch (SocketException)
            {
                // Port in use — try next.
            }
        }
        throw new InvalidOperationException(
            $"No free host port found in [{ProbeRangeStart}, {ProbeRangeEnd}] for RestartableSingleNodeFixture.");
    }
}

[CollectionDefinition("RestartableSingleNode")]
public class RestartableSingleNodeCollection : ICollectionFixture<RestartableSingleNodeFixture> { }
