using DotNet.Testcontainers.Builders;
using Testcontainers.ClickHouse;
using Xunit;

namespace CH.Native.StressTests.Fixtures;

/// <summary>
/// Shared test fixture that manages a ClickHouse container for stress tests.
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

    public string Host => _container.Hostname;
    public int Port => _container.GetMappedPublicPort(9000);
    public string Username => TestUsername;
    public string Password => TestPassword;
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

[CollectionDefinition("ClickHouse")]
public class ClickHouseCollection : ICollectionFixture<ClickHouseFixture>
{
}
