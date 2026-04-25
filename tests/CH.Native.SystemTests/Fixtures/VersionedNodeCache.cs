using CH.Native.Connection;
using DotNet.Testcontainers.Builders;
using Testcontainers.ClickHouse;
using Xunit;

namespace CH.Native.SystemTests.Fixtures;

/// <summary>
/// Lazily starts (and caches) one ClickHouse container per pinned image tag, so
/// version-matrix tests amortise startup cost across the whole class. The cache lives on
/// the xUnit assembly fixture so all version-matrix collections share it.
/// </summary>
public sealed class VersionedNodeCache : IAsyncLifetime
{
    private readonly Dictionary<string, ClickHouseContainer> _containers = new();
    private readonly SemaphoreSlim _lock = new(1, 1);

    public const string Username = "default";
    public const string Password = "test_password";

    public async Task<ClickHouseConnectionSettings> GetSettingsAsync(string image)
    {
        await _lock.WaitAsync();
        try
        {
            if (!_containers.TryGetValue(image, out var container))
            {
                container = new ClickHouseBuilder()
                    .WithImage(image)
                    .WithUsername(Username)
                    .WithPassword(Password)
                    .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9000))
                    .Build();
                await container.StartAsync();
                await WaitForHandshakeAsync(container);
                _containers[image] = container;
            }

            return ClickHouseConnectionSettings.CreateBuilder()
                .WithHost(container.Hostname)
                .WithPort(container.GetMappedPublicPort(9000))
                .WithCredentials(Username, Password)
                .Build();
        }
        finally
        {
            _lock.Release();
        }
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var container in _containers.Values)
            await container.DisposeAsync();
        _lock.Dispose();
    }

    private static async Task WaitForHandshakeAsync(ClickHouseContainer container)
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(container.Hostname)
            .WithPort(container.GetMappedPublicPort(9000))
            .WithCredentials(Username, Password)
            .Build();

        for (int i = 0; i < 20; i++)
        {
            try
            {
                await using var conn = new ClickHouseConnection(settings);
                await conn.OpenAsync();
                return;
            }
            catch when (i < 19)
            {
                await Task.Delay(500);
            }
        }
    }
}

[CollectionDefinition("VersionMatrix")]
public class VersionMatrixCollection : ICollectionFixture<VersionedNodeCache> { }
