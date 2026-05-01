namespace CH.Native.Connection;

/// <summary>
/// Tuning knobs for <see cref="ClickHouseDataSource"/>. Defaults mirror
/// <c>clickhouse-go</c>'s <c>MaxOpenConns=100</c> and Npgsql's 5-minute idle
/// timeout; the 30-minute lifetime is a conservative baseline that aligns
/// with typical JWT rotation windows.
/// </summary>
public sealed class ClickHouseDataSourceOptions
{
    /// <summary>
    /// Immutable baseline settings used to build physical connections. When
    /// <see cref="ConnectionFactory"/> is null, the pool builds every
    /// connection from this instance. Required.
    /// </summary>
    public required ClickHouseConnectionSettings Settings { get; init; }

    /// <summary>
    /// Optional per-create settings factory. When set, the pool invokes it
    /// every time it needs to build a brand-new physical connection (cache
    /// miss or lifetime expiry). This is the hook used by the DI layer to
    /// fetch rotating credentials (JWT, SSH key, mTLS cert, password) from
    /// their provider interfaces. Return a fresh <see cref="ClickHouseConnectionSettings"/>
    /// built from the baseline with the new credential applied.
    /// </summary>
    public Func<CancellationToken, ValueTask<ClickHouseConnectionSettings>>? ConnectionFactory { get; init; }

    /// <summary>Maximum number of physical connections (idle + busy). Default 100.</summary>
    public int MaxPoolSize { get; set; } = 100;

    /// <summary>
    /// Minimum number of physical connections to keep warm. When
    /// <see cref="PrewarmOnStart"/> is true, the pool fires this many
    /// <c>OpenAsync</c> calls on construction. Default 0.
    /// </summary>
    public int MinPoolSize { get; set; } = 0;

    /// <summary>
    /// Idle duration after which a connection is evicted on the next rent
    /// or background validation pass. Default 5 minutes.
    /// </summary>
    public TimeSpan ConnectionIdleTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum age of a physical connection regardless of activity. Critical
    /// for JWT rotation — set this to match (or undercut) token TTL so the
    /// pool recycles connections before credentials expire. Default 30 min.
    /// </summary>
    public TimeSpan ConnectionLifetime { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Maximum time to wait for a free slot when the pool is at
    /// <see cref="MaxPoolSize"/>. Default 30 seconds. Throws
    /// <see cref="TimeoutException"/> on exhaustion.
    /// </summary>
    public TimeSpan ConnectionWaitTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// When true, the pool runs <c>SELECT 1</c> on every rented connection
    /// before handing it to the caller. Off by default — the overhead usually
    /// outweighs the benefit, and consumers generally detect dead sockets on
    /// their own first query.
    /// </summary>
    public bool ValidateOnRent { get; set; } = false;

    /// <summary>
    /// When true, the DataSource fires <see cref="MinPoolSize"/> connection
    /// opens in the background on construction. Useful for latency-sensitive
    /// startups. Default false.
    /// </summary>
    public bool PrewarmOnStart { get; set; } = false;

    /// <summary>
    /// When true (default), the pool resets session-scoped server state on
    /// each connection return — restoring the default database, dropping
    /// dirty <c>SET</c> settings to <c>DEFAULT</c>, and dropping any
    /// <c>CREATE TEMPORARY TABLE</c>s the previous renter created. Without
    /// this, ClickHouse's session-state model leaks state from one renter
    /// to the next on a reused physical connection.
    ///
    /// Set to false to skip the reset (and accept the leak) when callers
    /// need the lowest-possible per-return latency or rely on session
    /// state surviving across rents (anti-pattern).
    /// </summary>
    public bool ResetSessionStateOnReturn { get; set; } = true;
}
