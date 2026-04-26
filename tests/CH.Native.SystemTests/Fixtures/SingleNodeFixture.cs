using CH.Native.Connection;
using DotNet.Testcontainers.Builders;
using Testcontainers.ClickHouse;
using Xunit;

namespace CH.Native.SystemTests.Fixtures;

/// <summary>
/// Single-node ClickHouse container shared across system tests that don't need a cluster.
/// Mirrors the integration-suite fixture but lives here so SystemTests is self-contained.
/// </summary>
public class SingleNodeFixture : IAsyncLifetime
{
    private const string TestUsername = "default";
    private const string TestPassword = "test_password";

    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:24.8")
        .WithUsername(TestUsername)
        .WithPassword(TestPassword)
        // Grant the default user access management so ServerFailures tests can run
        // CREATE USER / CREATE ROLE / CREATE SETTINGS PROFILE without grant errors.
        .WithEnvironment("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9000))
        .Build();

    public string Host => _container.Hostname;
    public int Port => _container.GetMappedPublicPort(9000);
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

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

[CollectionDefinition("SingleNode")]
public class SingleNodeCollection : ICollectionFixture<SingleNodeFixture> { }
