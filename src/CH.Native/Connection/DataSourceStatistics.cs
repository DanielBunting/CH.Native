namespace CH.Native.Connection;

/// <summary>
/// Point-in-time snapshot of a <see cref="ClickHouseDataSource"/>'s pool state.
/// Cheap to read; intended for diagnostics endpoints and health dashboards.
/// </summary>
/// <param name="Total">Connections owned by the pool — idle + busy.</param>
/// <param name="Idle">Connections currently available in the pool.</param>
/// <param name="Busy">Connections currently rented out to callers.</param>
/// <param name="PendingWaits">Callers queued behind <c>MaxPoolSize</c>.</param>
/// <param name="TotalRentsServed">Cumulative rents served since the pool started.</param>
/// <param name="TotalCreated">Cumulative physical connections created.</param>
/// <param name="TotalEvicted">Cumulative connections closed/discarded.</param>
public readonly record struct DataSourceStatistics(
    int Total,
    int Idle,
    int Busy,
    int PendingWaits,
    long TotalRentsServed,
    long TotalCreated,
    long TotalEvicted);
