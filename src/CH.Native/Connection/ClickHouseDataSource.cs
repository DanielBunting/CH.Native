using System.Collections.Concurrent;
using CH.Native.BulkInsert;

namespace CH.Native.Connection;

/// <summary>
/// Pooled, thread-safe factory for <see cref="ClickHouseConnection"/> instances.
/// Intended to be long-lived (singleton) in a typical application; each rent
/// is cheap because physical connections are reused across queries.
///
/// Consumers rent via <see cref="OpenConnectionAsync"/> and <c>await using</c>
/// the returned connection — disposing it returns the connection to the pool
/// instead of closing the socket.
/// </summary>
public sealed class ClickHouseDataSource : IAsyncDisposable
{
    private readonly ClickHouseDataSourceOptions _options;
    private readonly ConcurrentStack<PoolEntry> _idle = new();
    private readonly SemaphoreSlim _gate;
    private readonly CancellationTokenSource _disposeCts = new();
    private readonly Func<CancellationToken, ValueTask<ClickHouseConnectionSettings>> _settingsFactory;
    private int _total;
    private int _pendingWaits;
    private long _totalRentsServed;
    private long _totalCreated;
    private long _totalEvicted;
    private bool _disposed;

    /// <summary>
    /// Creates a DataSource from a connection string. Uses default pool knobs.
    /// </summary>
    public ClickHouseDataSource(string connectionString)
        : this(new ClickHouseDataSourceOptions { Settings = ClickHouseConnectionSettings.Parse(connectionString) })
    {
    }

    /// <summary>
    /// Creates a DataSource from pre-built settings. Uses default pool knobs.
    /// </summary>
    public ClickHouseDataSource(ClickHouseConnectionSettings settings)
        : this(new ClickHouseDataSourceOptions { Settings = settings })
    {
    }

    /// <summary>
    /// Creates a DataSource with custom pool options.
    /// </summary>
    public ClickHouseDataSource(ClickHouseDataSourceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.MaxPoolSize < 1)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxPoolSize must be at least 1.");
        if (options.MinPoolSize < 0 || options.MinPoolSize > options.MaxPoolSize)
            throw new ArgumentOutOfRangeException(nameof(options), "MinPoolSize must be between 0 and MaxPoolSize.");

        _options = options;
        _gate = new SemaphoreSlim(options.MaxPoolSize, options.MaxPoolSize);
        _settingsFactory = options.ConnectionFactory ?? (_ => new ValueTask<ClickHouseConnectionSettings>(options.Settings));

        if (options.PrewarmOnStart && options.MinPoolSize > 0)
        {
            _ = Task.Run(PrewarmAsync);
        }
    }

    /// <summary>The baseline connection settings.</summary>
    public ClickHouseConnectionSettings Settings => _options.Settings;

    /// <summary>The pool configuration.</summary>
    public ClickHouseDataSourceOptions Options => _options;

    /// <summary>
    /// Rents a connection from the pool, creating a fresh physical connection
    /// if no idle one is available. The returned <see cref="ClickHouseConnection"/>
    /// transparently returns itself to the pool on disposal.
    /// </summary>
    public async ValueTask<ClickHouseConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Gate semantics: each permit represents the right to hold a rented
        // connection. Permits = MaxPoolSize - busy. Acquired here on rent,
        // released in ReturnAsync (normal return) or DiscardAsync (dispose /
        // expiry). Idle connections sitting in _idle do NOT hold a permit,
        // which is what lets a returned connection wake a parked waiter.
        //
        // Link the caller's token with _disposeCts so a concurrent DisposeAsync
        // wakes parked waiters immediately instead of leaving them stuck on a
        // soon-to-be-disposed semaphore.
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, _disposeCts.Token);

        Interlocked.Increment(ref _pendingWaits);
        bool acquired;
        try
        {
            acquired = await _gate.WaitAsync(_options.ConnectionWaitTimeout, linkedCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_disposeCts.IsCancellationRequested)
        {
            // Disposal tripped the linked token. Surface the conventional error for a
            // post-dispose rent attempt rather than leaking OperationCanceledException.
            throw new ObjectDisposedException(nameof(ClickHouseDataSource));
        }
        finally
        {
            Interlocked.Decrement(ref _pendingWaits);
        }

        // Re-check after wake-up: a dispose that raced with us flips _disposed *before*
        // cancelling the token, so the waiter can see inconsistent state if it skips this.
        if (_disposed)
        {
            if (acquired)
            {
                try { _gate.Release(); } catch (ObjectDisposedException) { /* gate already gone */ }
            }
            throw new ObjectDisposedException(nameof(ClickHouseDataSource));
        }

        if (!acquired)
        {
            throw new TimeoutException(
                $"Timed out waiting for an available ClickHouse connection " +
                $"(MaxPoolSize={_options.MaxPoolSize}, wait={_options.ConnectionWaitTimeout}).");
        }

        try
        {
            // Prefer an idle connection. The permit we just acquired covers
            // the rent whether we pop an idle one or create a new one.
            while (_idle.TryPop(out var entry))
            {
                if (IsExpired(entry))
                {
                    // Cull without releasing the permit — it still belongs to this rent.
                    await DiscardInternalAsync(entry.Connection).ConfigureAwait(false);
                    continue;
                }
                if (_options.ValidateOnRent && !await PingAsync(entry.Connection, cancellationToken).ConfigureAwait(false))
                {
                    await DiscardInternalAsync(entry.Connection).ConfigureAwait(false);
                    continue;
                }
                AttachReturnHook(entry.Connection, entry.CreatedAt);
                Interlocked.Increment(ref _totalRentsServed);
                return entry.Connection;
            }

            // No idle available — create a fresh physical connection.
            var settings = await _settingsFactory(cancellationToken).ConfigureAwait(false);
            var conn = new ClickHouseConnection(settings);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            Interlocked.Increment(ref _total);
            Interlocked.Increment(ref _totalCreated);
            Interlocked.Increment(ref _totalRentsServed);
            AttachReturnHook(conn, DateTime.UtcNow);
            return conn;
        }
        catch
        {
            _gate.Release();
            throw;
        }
    }

    /// <summary>
    /// Non-throwing health probe: rents a connection, runs <c>SELECT 1</c>,
    /// returns true on success. Safe to call from an
    /// <c>IHealthCheck.CheckHealthAsync</c> implementation.
    /// </summary>
    public async ValueTask<bool> PingAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await using var conn = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            return await PingAsync(conn, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Creates a <see cref="BulkInserter{T}"/> backed by a pooled connection.
    /// The bulk inserter owns the connection for its entire lifetime; disposing
    /// the inserter returns the underlying connection to the pool.
    /// </summary>
    public async ValueTask<BulkInserter<T>> CreateBulkInserterAsync<T>(
        string tableName,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        var conn = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Ownership transfers to the BulkInserter. When the caller disposes
            // the inserter, the inserter's DisposeAsync disposes the connection,
            // which triggers the pool-return hook (installed by OpenConnectionAsync).
            return new BulkInserter<T>(conn, tableName, options);
        }
        catch
        {
            await conn.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Point-in-time snapshot of pool state.</summary>
    public DataSourceStatistics GetStatistics()
    {
        // Idle count is snapshot-only — ConcurrentStack.Count is an O(N) walk.
        // Accept the walk cost for diagnostics endpoints; callers should not
        // call this in a hot loop.
        var idle = _idle.Count;
        var total = Volatile.Read(ref _total);
        return new DataSourceStatistics(
            Total: total,
            Idle: idle,
            Busy: Math.Max(0, total - idle),
            PendingWaits: Volatile.Read(ref _pendingWaits),
            TotalRentsServed: Interlocked.Read(ref _totalRentsServed),
            TotalCreated: Interlocked.Read(ref _totalCreated),
            TotalEvicted: Interlocked.Read(ref _totalEvicted));
    }

    /// <summary>Disposes all pooled connections and blocks further rents.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Wake any parked waiters on the gate first so they observe _disposed=true on
        // the rebound and throw a clean ObjectDisposedException instead of either
        // blocking until _options.ConnectionWaitTimeout or hitting an already-disposed
        // semaphore.
        try { _disposeCts.Cancel(); } catch (ObjectDisposedException) { /* best-effort */ }

        while (_idle.TryPop(out var entry))
        {
            // Idle connections don't hold a gate permit (they were released on
            // return), so use the internal variant that skips Release() —
            // otherwise we'd either over-release or throw against a disposed gate.
            try { await DiscardInternalAsync(entry.Connection).ConfigureAwait(false); }
            catch { /* best-effort teardown */ }
        }

        // Intentionally do NOT call _gate.Dispose() or _disposeCts.Dispose(). Return
        // paths from in-flight rents may still touch _gate.Release() briefly after
        // we return, and the cancel-callback chain for waiters can post continuations
        // that read _disposeCts.IsCancellationRequested. SemaphoreSlim.Dispose only
        // frees an on-demand AvailableWaitHandle we never use, so skipping it is
        // cheap and removes an entire class of teardown races.
    }

    private void AttachReturnHook(ClickHouseConnection conn, DateTime createdAt)
    {
        conn.PoolReturnHook = c => ReturnAsync(c, createdAt);
    }

    private async ValueTask ReturnAsync(ClickHouseConnection conn, DateTime createdAt)
    {
        if (_disposed)
        {
            // Pool is torn down. The gate may already be disposed, so skip Release() —
            // there are no waiters on a disposed gate that need waking (they were
            // cancelled via _disposeCts) and calling Release would throw.
            await DiscardInternalAsync(conn).ConfigureAwait(false);
            return;
        }

        var age = DateTime.UtcNow - createdAt;
        if (age > _options.ConnectionLifetime || !conn.CanBePooled)
        {
            await DiscardAsync(conn).ConfigureAwait(false);
            return;
        }

        _idle.Push(new PoolEntry(conn, createdAt, DateTime.UtcNow));
        // Release the permit the renter was holding — this is what wakes
        // any waiter parked in OpenConnectionAsync, who will then pop the
        // idle entry we just pushed.
        try { _gate.Release(); }
        catch (ObjectDisposedException) { /* raced with DisposeAsync */ }
    }

    // Full discard: dispose, decrement _total, release the renter's gate permit.
    // Call this from the return path where the caller owns a permit.
    private async ValueTask DiscardAsync(ClickHouseConnection conn)
    {
        await DiscardInternalAsync(conn).ConfigureAwait(false);
        try { _gate.Release(); }
        catch (ObjectDisposedException) { /* raced with DisposeAsync */ }
    }

    // Discard without touching the gate. Used (a) while the rent path is culling
    // expired idle entries under a permit it still wants to keep, and (b) during
    // DataSource disposal when idle entries have no permit of their own to release.
    private async ValueTask DiscardInternalAsync(ClickHouseConnection conn)
    {
        try { await conn.DisposeAsync().ConfigureAwait(false); }
        catch { /* already in teardown; swallow */ }
        Interlocked.Decrement(ref _total);
        Interlocked.Increment(ref _totalEvicted);
    }

    private bool IsExpired(PoolEntry entry)
    {
        var now = DateTime.UtcNow;
        return now - entry.CreatedAt > _options.ConnectionLifetime
            || now - entry.LastUsedAt > _options.ConnectionIdleTimeout
            || !entry.Connection.IsOpen;
    }

    private static async ValueTask<bool> PingAsync(ClickHouseConnection conn, CancellationToken ct)
    {
        try
        {
            // Named arg to dodge the (string, object parameters, CancellationToken)
            // extension overload, which would otherwise bind ct to `parameters`.
            _ = await conn.ExecuteScalarAsync<int>("SELECT 1", cancellationToken: ct).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private async Task PrewarmAsync()
    {
        var target = _options.MinPoolSize;
        for (var i = 0; i < target; i++)
        {
            try
            {
                await using var conn = await OpenConnectionAsync(CancellationToken.None).ConfigureAwait(false);
                // Immediately disposing returns the connection to the pool.
            }
            catch
            {
                // Prewarm is best-effort; real failures surface on first real rent.
                break;
            }
        }
    }

    private readonly record struct PoolEntry(ClickHouseConnection Connection, DateTime CreatedAt, DateTime LastUsedAt);
}
