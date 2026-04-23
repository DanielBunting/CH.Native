using CH.Native.Connection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace CH.Native.DependencyInjection.HealthChecks;

/// <summary>
/// Registers <see cref="ClickHouseHealthCheck"/> on an <see cref="IHealthChecksBuilder"/>.
/// </summary>
public static class ClickHouseHealthCheckBuilderExtensions
{
    /// <summary>
    /// Adds a ClickHouse health check that pings the DataSource registered with
    /// the supplied <paramref name="serviceKey"/> (or the unkeyed DataSource when
    /// <paramref name="serviceKey"/> is null).
    /// </summary>
    /// <param name="builder">The health-checks builder.</param>
    /// <param name="name">Name of the health-check registration (shown in output).</param>
    /// <param name="serviceKey">Keyed-service key, or null for the default DataSource.</param>
    /// <param name="timeout">Per-check timeout. Default 5 seconds.</param>
    /// <param name="tags">Optional tags for filtering (e.g. <c>"ready"</c>).</param>
    public static IHealthChecksBuilder AddClickHouse(
        this IHealthChecksBuilder builder,
        string name = "clickhouse",
        object? serviceKey = null,
        TimeSpan? timeout = null,
        IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        return builder.Add(new HealthCheckRegistration(
            name,
            sp =>
            {
                var ds = serviceKey is null
                    ? sp.GetRequiredService<ClickHouseDataSource>()
                    : sp.GetRequiredKeyedService<ClickHouseDataSource>(serviceKey);
                return new ClickHouseHealthCheck(ds, timeout);
            },
            failureStatus: null,
            tags: tags));
    }
}
