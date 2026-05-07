using CH.Native.Connection;
using DotNet.Testcontainers.Builders;
using Testcontainers.ClickHouse;
using Xunit;

namespace CH.Native.Tests.Fixtures;

/// <summary>
/// Shared test fixture that manages a ClickHouse container for integration tests.
/// </summary>
public class ClickHouseFixture : IAsyncLifetime
{
    private const string TestUsername = "default";
    private const string TestPassword = "test_password";

    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:26.2")
        .WithUsername(TestUsername)
        .WithPassword(TestPassword)
        // Two-step readiness: TCP port + a successful query as the test user.
        // Port-only is too early in the boot sequence; under CI parallel-TFM
        // contention the test process can win the handshake race against a
        // still-warming auth subsystem and see "Server closed connection
        // during handshake".
        .WithWaitStrategy(Wait.ForUnixContainer()
            .UntilPortIsAvailable(9000)
            .UntilCommandIsCompleted(
                "clickhouse-client", "--user", TestUsername, "--password", TestPassword, "--query", "SELECT 1"))
        .Build();

    /// <summary>
    /// Gets the hostname of the ClickHouse container.
    /// </summary>
    public string Host => _container.Hostname;

    /// <summary>
    /// Gets the native protocol port (9000) mapped to the host.
    /// </summary>
    public int Port => _container.GetMappedPublicPort(9000);

    /// <summary>
    /// Gets the username for authentication.
    /// </summary>
    public string Username => TestUsername;

    /// <summary>
    /// Gets the password for authentication.
    /// </summary>
    public string Password => TestPassword;

    /// <summary>
    /// Gets a connection string for the native protocol.
    /// </summary>
    public string ConnectionString => $"Host={Host};Port={Port};Username={Username};Password={Password}";

    public async Task InitializeAsync()
    {
        await _container.StartAsync();

        // Belt-and-braces around the wait strategy: even with
        // UntilCommandIsCompleted above, the *external* handshake from the
        // test process can race a still-warming auth subsystem when 3+ TFMs
        // each spin up their own container concurrently on CI. 60×500ms = 30s
        // gives that race enough headroom without slowing the steady state.
        for (int attempt = 1; attempt <= 60; attempt++)
        {
            try
            {
                await using var connection = new ClickHouseConnection(ConnectionString);
                await connection.OpenAsync();
                return;
            }
            catch
            {
                if (attempt == 60) throw;
                await Task.Delay(500);
            }
        }
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}

/// <summary>
/// Collection definition for tests that share a single ClickHouse container.
/// </summary>
[CollectionDefinition("ClickHouse")]
public class ClickHouseCollection : ICollectionFixture<ClickHouseFixture>
{
}
