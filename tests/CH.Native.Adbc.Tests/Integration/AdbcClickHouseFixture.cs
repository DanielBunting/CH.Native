using CH.Native.Connection;
using DotNet.Testcontainers.Builders;
using Testcontainers.ClickHouse;
using Xunit;

namespace CH.Native.Adbc.Tests.Integration;

/// <summary>
/// Spins up a ClickHouse container for the ADBC integration tests and exposes a CH.Native
/// connection string (which the ADBC driver consumes via <see cref="AdbcOptionKeys.ConnectionString"/>).
/// </summary>
public class AdbcClickHouseFixture : IAsyncLifetime
{
    private const string TestUsername = "default";
    private const string TestPassword = "test_password";

    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:26.2")
        .WithUsername(TestUsername)
        .WithPassword(TestPassword)
        .WithWaitStrategy(Wait.ForUnixContainer()
            .UntilPortIsAvailable(9000)
            .UntilCommandIsCompleted(
                "clickhouse-client", "--user", TestUsername, "--password", TestPassword, "--query", "SELECT 1"))
        .Build();

    public string Host => _container.Hostname;

    public int Port => _container.GetMappedPublicPort(9000);

    public string ConnectionString =>
        $"Host={Host};Port={Port};Username={TestUsername};Password={TestPassword}";

    public async Task InitializeAsync()
    {
        await _container.StartAsync();

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

[CollectionDefinition("AdbcClickHouse")]
public class AdbcClickHouseCollection : ICollectionFixture<AdbcClickHouseFixture>
{
}
