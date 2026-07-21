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
        await StartWithRebindRetryAsync();
        await WaitForHandshakeAsync();
    }

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    /// <summary>Stops the underlying container. Pair with <see cref="StartContainerAsync"/>.</summary>
    public Task StopContainerAsync() => _container.StopAsync();

    /// <summary>
    /// Hard-kills the underlying container (SIGKILL to PID 1) — the equivalent of
    /// <c>docker kill --signal=KILL</c>. Distinct from <see cref="StopContainerAsync"/>
    /// (graceful SIGTERM): callers exercise the path where ClickHouse can't flush
    /// in-flight queries, can't drain TCP queues, and the kernel hands the client
    /// RST instead of FIN. Use to probe pool / reader behaviour under abrupt loss.
    /// </summary>
    public async Task KillContainerAsync()
    {
        var psi = new System.Diagnostics.ProcessStartInfo("docker", $"kill --signal=KILL {_container.Id}")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        using var p = System.Diagnostics.Process.Start(psi)
            ?? throw new InvalidOperationException("Could not start docker CLI");
        await p.WaitForExitAsync();
        if (p.ExitCode != 0)
        {
            var err = await p.StandardError.ReadToEndAsync();
            throw new InvalidOperationException(
                $"docker kill --signal=KILL {_container.Id} exited {p.ExitCode}: {err}");
        }
    }

    /// <summary>
    /// (Re)starts the container and waits for the ClickHouse server to accept a
    /// fresh handshake. The fixed host-port binding is preserved across stop/start,
    /// so callers' cached <see cref="ClickHouseConnectionSettings"/> remain valid.
    /// </summary>
    public async Task StartContainerAsync()
    {
        await StartWithRebindRetryAsync();
        await WaitForHandshakeAsync();
    }

    /// <summary>
    /// Starts <see cref="_container"/> on the pinned host port, tolerating the
    /// transient <c>address already in use</c> collision that occurs when the
    /// previous <c>docker-proxy</c> for that port has not been torn down yet —
    /// common right after <c>docker stop</c>, and worse after
    /// <c>docker kill --signal=KILL</c> (no graceful drain). The port is fixed by
    /// contract (cached settings must stay valid), so we wait for it to free and
    /// retry the SAME port rather than re-binding a different one.
    /// </summary>
    private async Task StartWithRebindRetryAsync()
    {
        // Give the prior userland proxy time to release the fixed host port before
        // the first attempt, so the common case doesn't even need a retry.
        await WaitForPortFreeAsync(_hostPort, TimeSpan.FromSeconds(15));

        const int maxAttempts = 6;
        var delay = TimeSpan.FromMilliseconds(500);
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                await _container.StartAsync();
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts && IsPortBindConflict(ex))
            {
                // The host port is still held by a lingering proxy. Back off, wait
                // for it to free, and retry the same fixed port.
                await Task.Delay(delay);
                await WaitForPortFreeAsync(_hostPort, TimeSpan.FromSeconds(10));
                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 4000));
            }
        }
    }

    /// <summary>
    /// Polls until <paramref name="port"/> is bindable on <see cref="IPAddress.Any"/>
    /// (matching Docker's <c>0.0.0.0</c> publish interface) or the timeout elapses.
    /// Best-effort: returns on timeout rather than throwing, leaving the actual
    /// bind outcome to <c>StartAsync</c>.
    /// </summary>
    private static async Task WaitForPortFreeAsync(int port, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            if (IsPortBindable(port))
                return;
            if (DateTime.UtcNow >= deadline)
                return;
            await Task.Delay(200);
        }
    }

    private static bool IsPortBindable(int port)
    {
        try
        {
            using var listener = new TcpListener(IPAddress.Any, port);
            listener.Start();
            listener.Stop();
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }

    /// <summary>
    /// True when <paramref name="ex"/> (or any inner exception — Testcontainers may
    /// wrap the <c>Docker.DotNet.DockerApiException</c>) is a host-port bind
    /// conflict. Scoped narrowly so genuine container failures surface immediately
    /// instead of being retried.
    /// </summary>
    private static bool IsPortBindConflict(Exception ex)
    {
        for (var e = ex; e is not null; e = e.InnerException)
        {
            var m = e.Message;
            if (m.Contains("address already in use", StringComparison.OrdinalIgnoreCase)
                || m.Contains("failed to bind host port", StringComparison.OrdinalIgnoreCase)
                || m.Contains("port is already allocated", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }
        return false;
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
        // Probe on IPAddress.Any (0.0.0.0), matching the interface Docker actually
        // publishes to. A port that binds here is genuinely free on every interface,
        // so we don't pick one that's free on loopback but held on 0.0.0.0 by a
        // lingering docker-proxy. The container's re-bind race is handled separately
        // by StartWithRebindRetryAsync.
        //
        // Start the scan at a randomized offset within the range so repeated fixture
        // constructions don't all contend on ProbeRangeStart first.
        int span = ProbeRangeEnd - ProbeRangeStart + 1;
        int offset = Random.Shared.Next(span);
        for (int i = 0; i < span; i++)
        {
            int port = ProbeRangeStart + ((offset + i) % span);
            if (IsPortBindable(port))
                return port;
        }
        throw new InvalidOperationException(
            $"No free host port found in [{ProbeRangeStart}, {ProbeRangeEnd}] for RestartableSingleNodeFixture.");
    }
}

[CollectionDefinition("RestartableSingleNode")]
public class RestartableSingleNodeCollection : ICollectionFixture<RestartableSingleNodeFixture> { }
