using CH.Native.Connection;

namespace CH.Native.Resilience;

/// <summary>
/// Monitors the health of multiple ClickHouse servers using background health checks.
/// </summary>
public sealed class HealthChecker : IAsyncDisposable
{
    private readonly IReadOnlyList<ServerNode> _nodes;
    private readonly ClickHouseConnectionSettings _baseSettings;
    private readonly TimeSpan _checkInterval;
    private readonly TimeSpan _healthCheckTimeout;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _backgroundTask;
    private bool _disposed;

    /// <summary>
    /// Occurs when a health check completes for a server.
    /// </summary>
    public event EventHandler<HealthCheckCompletedEventArgs>? OnHealthCheckCompleted;

    /// <summary>
    /// Gets the list of all server nodes being monitored.
    /// </summary>
    public IReadOnlyList<ServerNode> Nodes => _nodes;

    /// <summary>
    /// Creates a new health checker for the specified servers.
    /// </summary>
    /// <param name="servers">The server addresses to monitor.</param>
    /// <param name="baseSettings">The base connection settings (credentials, database, etc.).</param>
    /// <param name="checkInterval">The interval between health checks. Default is 10 seconds.</param>
    /// <param name="healthCheckTimeout">The timeout for individual health checks. Default is 5 seconds.</param>
    public HealthChecker(
        IEnumerable<ServerAddress> servers,
        ClickHouseConnectionSettings baseSettings,
        TimeSpan? checkInterval = null,
        TimeSpan? healthCheckTimeout = null)
    {
        _nodes = servers.Select(s => new ServerNode(s)).ToList();
        _baseSettings = baseSettings;
        _checkInterval = checkInterval ?? TimeSpan.FromSeconds(10);
        _healthCheckTimeout = healthCheckTimeout ?? TimeSpan.FromSeconds(5);

        // Start background health check task
        _backgroundTask = RunHealthChecksAsync(_cts.Token);
    }

    /// <summary>
    /// Gets all currently healthy server nodes.
    /// </summary>
    /// <returns>An enumerable of healthy server nodes.</returns>
    public IEnumerable<ServerNode> GetHealthyNodes()
    {
        return _nodes.Where(n => n.IsHealthy);
    }

    /// <summary>
    /// Gets the count of healthy servers.
    /// </summary>
    public int HealthyCount => _nodes.Count(n => n.IsHealthy);

    /// <summary>
    /// Manually triggers a health check for all servers.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task CheckAllAsync(CancellationToken cancellationToken = default)
    {
        await CheckAllServersAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task RunHealthChecksAsync(CancellationToken cancellationToken)
    {
        // Initial delay to allow application startup
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await CheckAllServersAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
                // Log and continue - don't let health check failures stop the background task
            }

            try
            {
                await Task.Delay(_checkInterval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task CheckAllServersAsync(CancellationToken cancellationToken)
    {
        var tasks = _nodes.Select(node => CheckServerAsync(node, cancellationToken));
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private async Task CheckServerAsync(ServerNode node, CancellationToken cancellationToken)
    {
        var wasHealthy = node.IsHealthy;
        var startTime = DateTime.UtcNow;
        Exception? checkException = null;
        var isHealthy = false;

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_healthCheckTimeout);

            var settings = CreateSettingsForNode(node.Address);
            await using var connection = new ClickHouseConnection(settings);
            await connection.OpenAsync(cts.Token).ConfigureAwait(false);

            var result = await connection.ExecuteScalarAsync<int>("SELECT 1", cancellationToken: cts.Token)
                .ConfigureAwait(false);

            if (result == 1)
            {
                node.MarkHealthy();
                isHealthy = true;
            }
            else
            {
                node.MarkUnhealthy();
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Don't mark as unhealthy if we're shutting down
            throw;
        }
        catch (Exception ex)
        {
            checkException = ex;
            node.MarkUnhealthy();
        }

        var duration = DateTime.UtcNow - startTime;
        OnHealthCheckCompleted?.Invoke(this, new HealthCheckCompletedEventArgs(
            node.Address,
            isHealthy,
            wasHealthy,
            duration,
            checkException));
    }

    private ClickHouseConnectionSettings CreateSettingsForNode(ServerAddress address)
    {
        return ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(address.Host)
            .WithPort(address.Port)
            .WithDatabase(_baseSettings.Database)
            .WithCredentials(_baseSettings.Username, _baseSettings.Password)
            .WithConnectTimeout(_baseSettings.ConnectTimeout)
            .WithClientName(_baseSettings.ClientName)
            .WithCompression(_baseSettings.Compress)
            .WithCompressionMethod(_baseSettings.CompressionMethod)
            .Build();
    }

    /// <summary>
    /// Disposes the health checker and stops background monitoring.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        _cts.Cancel();

        try
        {
            await _backgroundTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected during disposal
        }

        _cts.Dispose();
    }
}
