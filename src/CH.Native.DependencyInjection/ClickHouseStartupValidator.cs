using CH.Native.Connection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace CH.Native.DependencyInjection;

/// <summary>
/// IHostedService registered by <see cref="IClickHouseDataSourceBuilder.ValidateOnStart"/>.
/// Resolves the corresponding <see cref="ClickHouseDataSource"/> at host startup,
/// which forces the singleton factory to run and the deferred auth-pairing
/// validator to fire — so a misconfigured pairing surfaces at app startup
/// rather than at the first request that injects the DataSource.
/// </summary>
internal sealed class ClickHouseStartupValidator : IHostedService
{
    private readonly IServiceProvider _services;
    private readonly object? _serviceKey;

    public ClickHouseStartupValidator(IServiceProvider services, object? serviceKey)
    {
        _services = services;
        _serviceKey = serviceKey;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Resolution triggers CreateDataSource which calls ValidateAuthCredentialsOrThrow.
        _ = _serviceKey is null
            ? _services.GetRequiredService<ClickHouseDataSource>()
            : _services.GetRequiredKeyedService<ClickHouseDataSource>(_serviceKey);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
