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
        .WithImage("clickhouse/clickhouse-server:24.1")
        .WithUsername(TestUsername)
        .WithPassword(TestPassword)
        .WithWaitStrategy(Wait.ForUnixContainer()
            .UntilPortIsAvailable(9000))
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
