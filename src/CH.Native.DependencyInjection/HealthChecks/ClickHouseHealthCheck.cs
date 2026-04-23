using CH.Native.Connection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace CH.Native.DependencyInjection.HealthChecks;

/// <summary>
/// <see cref="IHealthCheck"/> adapter over <see cref="ClickHouseDataSource.PingAsync"/>.
/// Returns <c>Healthy</c> when the ping succeeds, <c>Unhealthy</c> otherwise.
/// Register via <see cref="ClickHouseHealthCheckBuilderExtensions.AddClickHouse(IHealthChecksBuilder,string,object?,TimeSpan?,IEnumerable{string}?)"/>.
/// </summary>
public sealed class ClickHouseHealthCheck : IHealthCheck
{
    private readonly ClickHouseDataSource _dataSource;
    private readonly TimeSpan _timeout;

    /// <summary>Creates a health check over the supplied DataSource.</summary>
    public ClickHouseHealthCheck(ClickHouseDataSource dataSource, TimeSpan? timeout = null)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _timeout = timeout ?? TimeSpan.FromSeconds(5);
    }

    /// <inheritdoc />
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(_timeout);
        try
        {
            var ok = await _dataSource.PingAsync(linked.Token).ConfigureAwait(false);
            return ok
                ? HealthCheckResult.Healthy("ClickHouse SELECT 1 succeeded")
                : HealthCheckResult.Unhealthy("ClickHouse SELECT 1 failed");
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return HealthCheckResult.Unhealthy($"ClickHouse health check timed out after {_timeout}");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("ClickHouse health check threw", ex);
        }
    }
}
