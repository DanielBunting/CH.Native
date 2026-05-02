using System.Buffers;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Exceptions;
using CH.Native.Mapping;
using CH.Native.Sql;
using CH.Native.Telemetry;

namespace CH.Native.BulkInsert;

/// <summary>
/// Provides high-performance bulk insert operations using the ClickHouse native protocol.
/// </summary>
/// <typeparam name="T">The POCO type representing a row. Must be a class with a parameterless constructor.</typeparam>
public sealed class BulkInserter<T> : IAsyncDisposable where T : class
{
    private readonly ClickHouseConnection _connection;
    private readonly string _tableName;
    private readonly BulkInsertOptions _options;
    private readonly List<T> _buffer;

    private string[]? _columnNames;
    private string[]? _columnTypes;
    private Func<T, object?>[]? _getters;
    private bool _initialized;
    private bool _completed;
    private bool _completeStarted;
    private bool _disposed;
    // True between EnterBusyForBulkInsert and the matching ExitBusy in
    // DisposeAsync. Lets DisposeAsync release the busy slot exactly once
    // even when InitAsync threw before _initialized flipped.
    private bool _slotClaimed;
    private int _totalRowsInserted;
    private bool _usedCachedSchema;
    private SchemaKey _schemaCacheKey;
    private string? _effectiveQueryId;

    // Pooled column data arrays - reused across flushes (fallback path)
    private object?[][]? _pooledColumnData;
    private int _pooledArraySize;

    // Direct-to-buffer extractors - avoid boxing for most types
    private IColumnExtractor<T>[]? _extractors;
    private bool _useDirectPath;

    /// <summary>
    /// Creates a new bulk inserter for the specified table.
    /// </summary>
    /// <param name="connection">The ClickHouse connection to use.</param>
    /// <param name="tableName">The table name to insert into.</param>
    /// <param name="options">Optional bulk insert options.</param>
    public BulkInserter(ClickHouseConnection connection, string tableName, BulkInsertOptions? options = null)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        _options = options ?? BulkInsertOptions.Default;
        if (_options.BatchSize <= 0)
            throw new ArgumentOutOfRangeException(
                nameof(options),
                _options.BatchSize,
                $"{nameof(BulkInsertOptions)}.{nameof(BulkInsertOptions.BatchSize)} must be greater than zero.");
        _buffer = new List<T>(_options.BatchSize);
    }

    /// <summary>
    /// Gets the number of rows currently buffered.
    /// </summary>
    public int BufferedCount => _buffer.Count;

    /// <summary>
    /// Suppresses the dispose-time "unflushed rows" failure for callers that
    /// already know the wire is being torn down (e.g. cancellation: the
    /// connection-level drain has already finalised protocol state, so retrying
    /// the implicit complete in <see cref="DisposeAsync"/> would race a server
    /// that has already moved on).
    /// </summary>
    internal void Abort()
    {
        _completeStarted = true;
        _buffer.Clear();
    }

    /// <summary>
    /// Synchronous-cancel observer for the post-initialised insert path.
    /// </summary>
    /// <remarks>
    /// Replaces <c>cancellationToken.ThrowIfCancellationRequested()</c> in
    /// methods that run after <see cref="InitAsync"/> has put the wire into
    /// INSERT state. A bare <c>ThrowIfCancellationRequested</c> here would
    /// poison the connection: the server is waiting for the terminator block,
    /// the client has thrown OCE without sending Cancel, and the next query on
    /// the same connection reads the orphaned response. Sending Cancel + drain
    /// before re-throwing realigns the wire so the connection is reusable.
    /// Pre-init paths must use the plain throw — no Cancel should be sent
    /// before the INSERT query has been issued.
    /// </remarks>
    private ValueTask ObserveCancellationAsync(CancellationToken cancellationToken)
    {
        // Synchronous fast path: when the token can't be cancelled (the common
        // CancellationToken.None case in AddAsync's per-row hot loop), or when
        // it's live but not yet fired, return a completed ValueTask so the
        // async state machine in the caller stays allocation-free.
        if (!cancellationToken.CanBeCanceled || !cancellationToken.IsCancellationRequested)
            return ValueTask.CompletedTask;

        return ObserveCancellationSlowPathAsync(cancellationToken);
    }

    private async ValueTask ObserveCancellationSlowPathAsync(CancellationToken cancellationToken)
    {
        await _connection.SendCancelAsync().ConfigureAwait(false);
        Abort();
        await _connection.DrainAfterCancellationAsync().ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
    }

    /// <summary>
    /// Registers <see cref="ClickHouseConnection.SendCancelAsync"/> to fire on
    /// token cancellation, returning <c>default</c> for non-cancellable tokens
    /// so the bulk-insert hot path skips the registration ceremony entirely
    /// (closure + linked-list-node allocation otherwise charged per I/O call).
    /// Caller disposes the registration via <c>using</c>.
    /// </summary>
    private CancellationTokenRegistration RegisterCancelHook(CancellationToken cancellationToken)
    {
        return cancellationToken.CanBeCanceled
            ? cancellationToken.Register(static state => _ = ((ClickHouseConnection)state!).SendCancelAsync(), _connection)
            : default;
    }

    /// <summary>
    /// Initializes the bulk inserter by sending the INSERT query and receiving the schema.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <remarks>
    /// When <see cref="BulkInsertOptions.UseSchemaCache"/> is enabled and a matching schema
    /// is cached on the connection, this method still sends the INSERT query (required for
    /// server-side protocol context) but skips the synchronous wait for the schema Data
    /// block. The server's schema response is drained later by
    /// <see cref="ClickHouseConnection.ReceiveEndOfStreamAsync"/>, saving one round-trip.
    /// </remarks>
    public async Task InitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_initialized)
            throw new InvalidOperationException("BulkInserter is already initialized.");

        // Pre-INSERT: nothing on the wire yet, so a cancelled token at entry
        // is a plain throw — no Cancel packet to send, no drain to perform.
        // Must check before the registration is set up: Register on an already-
        // cancelled token fires synchronously, which would dispatch SendCancel
        // against a connection that hasn't even sent the INSERT query.
        cancellationToken.ThrowIfCancellationRequested();

        // Bulk insert holds the connection's wire from InitAsync until
        // CompleteAsync/DisposeAsync, so claim the busy slot for the lifetime
        // of the inserter. A concurrent QueryAsync on the same connection
        // throws ClickHouseConnectionBusyException instead of corrupting the
        // INSERT byte stream. Resolve the query id once so the same id is
        // surfaced in the busy exception, sent on the wire, and logged by
        // the server.
        var effectiveQueryId = ClickHouseConnection.ResolveQueryIdInternal(_options.QueryId);
        _effectiveQueryId = effectiveQueryId;
        _connection.EnterBusyForBulkInsert(effectiveQueryId);
        _slotClaimed = true;

        using var registration = RegisterCancelHook(cancellationToken);
        try
        {
            // Build column list from POCO properties. Each identifier is quoted via
            // ClickHouseIdentifier.Quote so a hostile [ClickHouseColumn(Name=...)] value
            // can never break out of the INSERT statement.
            var propertyMappings = GetPropertyMappings();
            var columnList = string.Join(", ", propertyMappings.Select(p => ClickHouseIdentifier.Quote(p.ColumnName)));
            _schemaCacheKey = new SchemaKey(_tableName, columnList);

            // Send INSERT query (required for server-side protocol state even on cache hit)
            var sql = $"INSERT INTO {ClickHouseIdentifier.Quote(_tableName)} ({columnList}) VALUES";
            // Snapshot to IReadOnlyList so the wire path doesn't observe later mutation.
            var rolesSnapshot = _options.Roles is null ? null : (IReadOnlyList<string>)_options.Roles.ToArray();
            await _connection.SendInsertQueryAsync(
                sql,
                cancellationToken,
                rolesOverride: rolesSnapshot,
                queryId: effectiveQueryId);

            // Per-call override wins; null falls back to the connection setting.
            var useCache = _options.UseSchemaCache ?? _connection.Settings.UseSchemaCache;

            if (useCache &&
                _connection.SchemaCache.TryGet(_schemaCacheKey, out var cachedSchema))
            {
                // Cache hit: use cached schema, skip the schema read round-trip.
                // The server's schema Data block will be drained by ReceiveEndOfStreamAsync.
                MapPropertiesToSchema(propertyMappings, cachedSchema.ColumnNames, cachedSchema.ColumnTypes);
                _usedCachedSchema = true;
                _connection.Logger.BulkInsertSchemaFetched(_tableName, cachedSchema.ColumnNames.Length, fromCache: true);
            }
            else
            {
                // Cache miss (or caching disabled): wait for server schema, then map.
                var schemaBlock = await _connection.ReceiveSchemaBlockAsync(cancellationToken);
                var names = schemaBlock.ColumnNames;
                var types = schemaBlock.ColumnTypes;
                MapPropertiesToSchema(propertyMappings, names, types);

                if (useCache)
                {
                    _connection.SchemaCache.Set(_schemaCacheKey, new BulkInsertSchema(names, types));
                }

                _connection.Logger.BulkInsertSchemaFetched(_tableName, names.Length, fromCache: false);
            }

            _initialized = true;
        }
        catch (OperationCanceledException) when (_connection.WasCancellationRequested)
        {
            // SendCancelAsync wrote the Cancel packet; drain the server's
            // response so the wire is realigned for connection reuse.
            Abort();
            await _connection.DrainAfterCancellationAsync();
            ReleaseSlotIfClaimed();
            throw;
        }
        catch
        {
            // Any other failure leaves the inserter unusable. Server-side
            // failures (table missing, etc.) arrive as a server-exception
            // envelope without a trailing EndOfStream, so the read loop
            // exits with _currentQueryId still set. Clear it here so the
            // connection's pool eligibility recovers — otherwise CanBePooled
            // returns false and a perfectly clean connection gets discarded.
            _connection.ClearOwnedQueryId(effectiveQueryId);
            ReleaseSlotIfClaimed();
            throw;
        }
    }

    private void ReleaseSlotIfClaimed()
    {
        if (!_slotClaimed) return;
        _slotClaimed = false;
        _connection.ExitBusy();
    }

    /// <summary>
    /// Adds a single item to the buffer. Automatically flushes when batch size is reached.
    /// </summary>
    /// <param name="item">The item to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask AddAsync(T item, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("BulkInserter must be initialized before adding items.");
        if (_completed)
            throw new InvalidOperationException("BulkInserter has been completed and cannot accept more items.");
        // After a cancelled or failed CompleteAsync, the wire's INSERT context
        // is torn down and any rows accepted into the buffer would be silently
        // dropped at dispose. Reject loudly — the caller has a programming
        // error, not a transient condition.
        if (_completeStarted)
            throw new InvalidOperationException(
                "BulkInserter cannot accept more items after a cancelled or failed CompleteAsync. " +
                "Create a new BulkInserter to retry.");

        // Honor cancellation at the boundary so a cancelled token reliably
        // aborts the in-progress insert. Without this, AddAsync is purely
        // synchronous between flushes and would never observe the token on a
        // fast/local connection — letting the caller silently commit data
        // they asked us to abandon. ObserveCancellationAsync also drains the
        // wire so the connection is reusable after the throw.
        await ObserveCancellationAsync(cancellationToken).ConfigureAwait(false);

        _buffer.Add(item);

        if (_buffer.Count >= _options.BatchSize)
        {
            await FlushAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Adds multiple items to the buffer. Automatically flushes when batch size is reached.
    /// </summary>
    /// <param name="items">The items to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask AddRangeAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("BulkInserter must be initialized before adding items.");
        if (_completed)
            throw new InvalidOperationException("BulkInserter has been completed and cannot accept more items.");
        if (_completeStarted)
            throw new InvalidOperationException(
                "BulkInserter cannot accept more items after a cancelled or failed CompleteAsync. " +
                "Create a new BulkInserter to retry.");

        await ObserveCancellationAsync(cancellationToken).ConfigureAwait(false);

        foreach (var item in items)
        {
            await ObserveCancellationAsync(cancellationToken).ConfigureAwait(false);

            _buffer.Add(item);

            if (_buffer.Count >= _options.BatchSize)
            {
                await FlushAsync(cancellationToken);
            }
        }
    }

    /// <summary>
    /// Adds multiple items using direct-to-wire streaming with reduced GC pressure.
    /// This method bypasses the internal List buffer and uses pooled arrays instead,
    /// reducing Gen1 GC collections for large streaming inserts.
    /// </summary>
    /// <param name="items">The items to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask AddRangeStreamingAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("BulkInserter must be initialized before adding items.");
        if (_completed)
            throw new InvalidOperationException("BulkInserter has been completed and cannot accept more items.");
        if (_completeStarted)
            throw new InvalidOperationException(
                "BulkInserter cannot accept more items after a cancelled or failed CompleteAsync. " +
                "Create a new BulkInserter to retry.");

        await ObserveCancellationAsync(cancellationToken).ConfigureAwait(false);

        // If we don't have the direct path available, fall back to regular AddRangeAsync
        if (!_useDirectPath || _extractors == null)
        {
            await AddRangeAsync(items, cancellationToken);
            return;
        }

        // Use pooled array to accumulate rows without long-lived List<T> references
        var batchSize = _options.BatchSize;
        var batch = ArrayPool<T>.Shared.Rent(batchSize);

        using var registration = RegisterCancelHook(cancellationToken);
        try
        {
            var batchIndex = 0;

            foreach (var item in items)
            {
                await ObserveCancellationAsync(cancellationToken).ConfigureAwait(false);

                batch[batchIndex++] = item;

                if (batchIndex >= batchSize)
                {
                    // Send directly without copying to List<T>
                    await _connection.SendDataBlockDirectAsync(
                        _extractors,
                        batch,
                        batchIndex,
                        cancellationToken);

                    // Clear references immediately to allow GC of row objects
                    Array.Clear(batch, 0, batchIndex);
                    batchIndex = 0;
                }
            }

            // Flush any remaining rows
            if (batchIndex > 0)
            {
                await _connection.SendDataBlockDirectAsync(
                    _extractors,
                    batch,
                    batchIndex,
                    cancellationToken);
            }
        }
        catch (OperationCanceledException) when (_connection.WasCancellationRequested)
        {
            Abort();
            await _connection.DrainAfterCancellationAsync();
            throw;
        }
        finally
        {
            // Return pooled array with clearing to release object references
            ArrayPool<T>.Shared.Return(batch, clearArray: true);
        }
    }

    /// <summary>
    /// Adds multiple items from an async enumerable using direct-to-wire streaming with reduced GC pressure.
    /// This method bypasses the internal List buffer and uses pooled arrays instead,
    /// reducing Gen1 GC collections for large streaming inserts.
    /// </summary>
    /// <param name="items">The async enumerable of items to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask AddRangeStreamingAsync(IAsyncEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("BulkInserter must be initialized before adding items.");
        if (_completed)
            throw new InvalidOperationException("BulkInserter has been completed and cannot accept more items.");
        if (_completeStarted)
            throw new InvalidOperationException(
                "BulkInserter cannot accept more items after a cancelled or failed CompleteAsync. " +
                "Create a new BulkInserter to retry.");

        await ObserveCancellationAsync(cancellationToken).ConfigureAwait(false);

        // If we don't have the direct path available, fall back to adding one by one
        if (!_useDirectPath || _extractors == null)
        {
            await foreach (var item in items.WithCancellation(cancellationToken))
            {
                await AddAsync(item, cancellationToken);
            }
            return;
        }

        // Use pooled array to accumulate rows without long-lived List<T> references
        var batchSize = _options.BatchSize;
        var batch = ArrayPool<T>.Shared.Rent(batchSize);

        using var registration = RegisterCancelHook(cancellationToken);
        try
        {
            var batchIndex = 0;

            await foreach (var item in items.WithCancellation(cancellationToken))
            {
                batch[batchIndex++] = item;

                if (batchIndex >= batchSize)
                {
                    // Send directly without copying to List<T>
                    await _connection.SendDataBlockDirectAsync(
                        _extractors,
                        batch,
                        batchIndex,
                        cancellationToken);

                    // Clear references immediately to allow GC of row objects
                    Array.Clear(batch, 0, batchIndex);
                    batchIndex = 0;
                }
            }

            // Flush any remaining rows
            if (batchIndex > 0)
            {
                await _connection.SendDataBlockDirectAsync(
                    _extractors,
                    batch,
                    batchIndex,
                    cancellationToken);
            }
        }
        catch (OperationCanceledException) when (_connection.WasCancellationRequested)
        {
            Abort();
            await _connection.DrainAfterCancellationAsync();
            throw;
        }
        finally
        {
            // Return pooled array with clearing to release object references
            ArrayPool<T>.Shared.Return(batch, clearArray: true);
        }
    }

    /// <summary>
    /// Flushes the current buffer to the server.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("BulkInserter must be initialized before flushing.");
        // No `_completeStarted` guard here: CompleteAsync flips that flag
        // before calling this method internally, and the AddAsync family
        // already prevents new rows from landing post-cancel — so the
        // buffer is provably empty and the natural empty-buffer no-op at
        // the bottom of this method handles the post-cancel case correctly.

        await ObserveCancellationAsync(cancellationToken).ConfigureAwait(false);

        if (_buffer.Count == 0)
            return;

        using var activity = ClickHouseActivitySource.Source.StartActivity("clickhouse.bulk_insert.flush", ActivityKind.Client);
        if (activity != null)
        {
            activity.SetTag("db.system", "clickhouse");
            activity.SetTag("db.clickhouse.table", _tableName);
            activity.SetTag("db.clickhouse.rows", _buffer.Count);
        }

        using var registration = RegisterCancelHook(cancellationToken);
        try
        {
            if (_useDirectPath && _extractors != null)
            {
                // Fast path: direct-to-buffer writing (no boxing)
                await _connection.SendDataBlockDirectAsync(
                    _extractors,
                    _buffer,
                    _buffer.Count,
                    cancellationToken);
            }
            else
            {
                // Fallback path: extract column data from buffer (with boxing)
                var columnData = ExtractColumnData();

                // Send data block
                await _connection.SendDataBlockAsync(
                    _columnNames!,
                    _columnTypes!,
                    columnData,
                    _buffer.Count,
                    cancellationToken);
            }

            _totalRowsInserted += _buffer.Count;
            _connection.Logger.BulkInsertFlushed(_tableName, _buffer.Count);

            // Emit the per-flush row count to the documented bulk-insert
            // counter. Tags mirror the bulk-insert span attributes so consumers
            // can correlate metrics with traces. The counter was previously
            // defined but never incremented — pin via BulkInsertMetricsTests.
            CH.Native.Telemetry.ClickHouseMeter.RowsWrittenTotal.Add(
                _buffer.Count,
                new KeyValuePair<string, object?>("db.system", "clickhouse"),
                new KeyValuePair<string, object?>("db.name", _connection.Settings.Database),
                new KeyValuePair<string, object?>("db.clickhouse.table", _tableName));
        }
        catch (OperationCanceledException ex) when (_connection.WasCancellationRequested)
        {
            ClickHouseActivitySource.SetError(activity, ex);
            Abort();
            await _connection.DrainAfterCancellationAsync();
            throw;
        }
        catch (Exception ex)
        {
            ClickHouseActivitySource.SetError(activity, ex);
            // Don't drain the wire here, and don't call Abort. Two contracts
            // depend on this:
            //  - BulkInsertBufferSurvivalTests.Flush_RowMapperException_BufferRetainsRows
            //    requires _buffer untouched and _completeStarted=false so the
            //    caller can retry on a fresh connection.
            //  - BulkInsertExtractionFailureTests.Extraction_DelegateThrows_ConnectionRemainsUsable
            //    requires the connection to be reusable, but its DisposeAsync
            //    catch already drives the wire-finalisation path
            //    (SendEmptyBlock + ReceiveEndOfStream) via DisposeAsync's
            //    "unflushed rows" branch — that's where the wire gets cleaned
            //    up. A drain here would race that path and leave Dispose's
            //    SendEmptyBlock to land on an idle server, poisoning the wire.
            throw;
        }

        _buffer.Clear();
    }

    /// <summary>
    /// Completes the bulk insert operation by flushing any remaining data and finalizing.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("BulkInserter must be initialized before completing.");
        // Idempotent: a successful complete short-circuits future calls (the
        // wire is back to idle), and a previously-attempted-but-cancelled
        // complete must NOT re-run the wire sequence — the server already
        // processed the Cancel packet and any further Data packets would be
        // rejected with UNEXPECTED_PACKET_FROM_CLIENT, poisoning the
        // connection. The DisposeAsync path at the end of this file follows
        // the same convention (skips re-complete when _completeStarted is set).
        if (_completed || _completeStarted)
            return;

        // ClickHouse's native protocol commits an INSERT only when the empty
        // terminator block lands. If the caller passed a cancelled token, we
        // must NOT send the buffered remainder or the terminator — doing so
        // would commit work the caller asked us to abandon. ObserveCancellation
        // also drains the wire so the connection is reusable after the throw.
        await ObserveCancellationAsync(cancellationToken).ConfigureAwait(false);

        // Mark the attempt so Dispose knows not to retry after a failed complete
        // (retry would re-invoke ReceiveEndOfStreamAsync on a broken wire and mask
        // the original server/protocol exception).
        _completeStarted = true;

        using var activity = ClickHouseActivitySource.StartBulkInsert(
            _tableName,
            _connection.Settings.Database,
            _effectiveQueryId,
            _connection.Settings.Telemetry);

        using var registration = RegisterCancelHook(cancellationToken);
        try
        {
            // Flush any remaining items
            await FlushAsync(cancellationToken);

            // Send empty block to signal end of data
            await _connection.SendEmptyBlockAsync(cancellationToken);

            // Wait for server confirmation
            await _connection.ReceiveEndOfStreamAsync(cancellationToken);

            activity?.SetTag("db.clickhouse.rows", _totalRowsInserted);

            _completed = true;

            // Release the busy slot now: the wire is back to idle and the
            // caller is free to issue another query before disposing the
            // inserter. DisposeAsync's ReleaseSlotIfClaimed is a no-op on
            // a re-entry because _slotClaimed is now false.
            ReleaseSlotIfClaimed();
        }
        catch (ClickHouseServerException ex) when (_usedCachedSchema)
        {
            // Server-side rejection on the cached-schema path is almost always schema drift
            // (e.g. post-ALTER column removal, rename, or type change). Evict the entry so
            // the next inserter refreshes, and surface the error to the caller.
            _connection.SchemaCache.InvalidateTable(_tableName);
            ClickHouseActivitySource.SetError(activity, ex);
            throw;
        }
        catch (OperationCanceledException ex) when (_connection.WasCancellationRequested)
        {
            ClickHouseActivitySource.SetError(activity, ex);
            // FlushAsync's own catch may have already drained; if so, draining
            // again is a no-op (the queryId is already cleared and the wire
            // realigned). Calling unconditionally keeps CompleteAsync robust
            // when the OCE escaped a non-flush write (SendEmptyBlockAsync /
            // ReceiveEndOfStreamAsync) without going through FlushAsync's catch.
            Abort();
            await _connection.DrainAfterCancellationAsync();
            throw;
        }
        catch (Exception ex)
        {
            ClickHouseActivitySource.SetError(activity, ex);
            // Wire is mid-INSERT and Dispose's "unflushed rows" recovery is gated off
            // by _completeStarted. Mark the connection fatal so a subsequent call
            // fails fast with a clear "broken connection" message rather than
            // landing on the dirty wire and producing a cryptic protocol error.
            _connection.MarkProtocolFatal();
            throw;
        }
    }

    /// <summary>
    /// Disposes the bulk inserter. If buffered (un-flushed) rows exist at dispose
    /// time without an explicit <see cref="CompleteAsync"/>, an
    /// <see cref="InvalidOperationException"/> is thrown so the data-loss is loud
    /// rather than silent.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Important commit semantics.</b> Each batch that <see cref="AddAsync"/>
    /// auto-flushes (or that <see cref="FlushAsync"/> writes explicitly) is
    /// committed by the server <em>per-block</em> — they're durable at the moment
    /// the block lands, independent of whether the INSERT statement is "completed"
    /// by the empty-terminator block. So when <c>DisposeAsync</c> runs without
    /// <c>CompleteAsync</c>, the previously-flushed batches <b>are persisted</b>;
    /// only the buffered remainder (rows added since the last flush) is lost.
    /// The exception message wording reflects this — "the buffered remainder
    /// will not be persisted", not "no rows were persisted".
    /// </para>
    /// <para>
    /// Callers who need true all-or-nothing semantics across an entire batch
    /// stream should use a <c>BatchSize</c> larger than the total row count
    /// they intend to insert (so nothing flushes until <c>CompleteAsync</c>),
    /// or wrap the whole insert in their own try/catch and re-issue against
    /// a fresh table on failure.
    /// </para>
    /// <para>
    /// If the caller did not call <see cref="CompleteAsync"/> but also never
    /// added rows (e.g. <see cref="InitAsync"/> threw, or the inserter was
    /// abandoned before any <see cref="AddAsync"/>), Dispose still needs to
    /// finalize the server-side protocol state so the underlying connection
    /// is reusable. It does so by driving the implicit complete via
    /// <see cref="CompleteAsync"/>. A failure there still surfaces to the
    /// caller — a broken wire is not something to swallow.
    /// </para>
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        // CompleteAsync guards on ObjectDisposedException.ThrowIf(_disposed, ...), so
        // the dispose flag MUST be flipped after the complete call runs; otherwise the
        // implicit complete is a silent no-op and the INSERT is left half-sent.
        try
        {
            if (_initialized && !_completed && !_completeStarted)
            {
                if (_buffer.Count > 0)
                {
                    // Buffered rows + no explicit complete = loud failure. Finalize the
                    // wire by sending the empty end-of-stream block so the connection is
                    // reusable, then throw.
                    //
                    // Important: ClickHouse INSERT commits each data block independently
                    // (see the BulkInsertAtomicityTests suite). Any batch that was
                    // already flushed before this dispose IS persistent; only the
                    // buffered remainder (the rows below) is dropped. The exception
                    // message reflects that — saying "no rows persisted" would be
                    // a lie.
                    var bufferedCount = _buffer.Count;
                    var totalFlushedRows = _totalRowsInserted;
                    _buffer.Clear();
                    try { await _connection.SendEmptyBlockAsync(CancellationToken.None).ConfigureAwait(false); } catch { /* best-effort teardown */ }
                    try { await _connection.ReceiveEndOfStreamAsync(CancellationToken.None).ConfigureAwait(false); } catch { /* best-effort teardown */ }

                    throw new InvalidOperationException(
                        $"BulkInserter for table '{_tableName}' was disposed with {bufferedCount} " +
                        $"un-flushed row(s) and no call to CompleteAsync(). Those rows are LOST. " +
                        $"({totalFlushedRows} previously-flushed row(s) on this inserter ARE persisted — " +
                        $"ClickHouse commits each data block independently of CompleteAsync.) " +
                        $"Call CompleteAsync() explicitly before disposing to flush the buffer.");
                }

                // No buffered rows but still mid-stream: finalize protocol state so the
                // underlying connection remains reusable. Failures here propagate — a
                // broken wire is a data-stability concern worth surfacing.
                await CompleteAsync().ConfigureAwait(false);
            }
            // If _completeStarted is true the caller already attempted CompleteAsync
            // (success or failure). Don't retry — a retry would re-read the wire and
            // mask the original exception the caller already observed.
        }
        finally
        {
            _disposed = true;
            ReturnPooledArrays();
            _buffer.Clear();
            ReleaseSlotIfClaimed();
        }
    }

    private List<PropertyMapping> GetPropertyMappings()
    {
        // Use reflection-based mapping
        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead)
            .Select(p =>
            {
                // Check deprecated ColumnAttribute
#pragma warning disable CS0618 // Type or member is obsolete
                var legacyAttr = p.GetCustomAttribute<ColumnAttribute>();
                if (legacyAttr?.Ignore == true)
                    return null;
#pragma warning restore CS0618

                // Check new ClickHouseColumnAttribute
                var newAttr = p.GetCustomAttribute<ClickHouseColumnAttribute>();
                if (newAttr?.Ignore == true)
                    return null;

                // Prefer new attribute, fall back to legacy
                var columnName = newAttr?.Name ?? legacyAttr?.Name ?? p.Name;
                var order = newAttr?.Order ?? legacyAttr?.Order ?? int.MaxValue;

                return new PropertyMapping
                {
                    Property = p,
                    ColumnName = columnName,
                    Order = order
                };
            })
            .Where(m => m != null)
            .Cast<PropertyMapping>()
            .OrderBy(m => m.Order)
            .ThenBy(m => m.ColumnName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (properties.Count == 0)
            throw new InvalidOperationException($"Type {typeof(T).Name} has no readable properties for bulk insert.");

        return properties;
    }

    private void MapPropertiesToSchema(
        List<PropertyMapping> propertyMappings,
        string[] schemaColumnNames,
        string[] schemaColumnTypes)
    {
        // Build lookup from schema
        var schemaColumns = new Dictionary<string, (int Index, string Type)>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < schemaColumnNames.Length; i++)
        {
            schemaColumns[schemaColumnNames[i]] = (i, schemaColumnTypes[i]);
        }

        // Match properties to schema columns
        var matchedNames = new List<string>();
        var matchedTypes = new List<string>();
        var matchedGetters = new List<Func<T, object?>>();
        var matchedExtractors = new List<IColumnExtractor<T>>();
        var canUseDirectPath = true;

        foreach (var mapping in propertyMappings)
        {
            if (!schemaColumns.TryGetValue(mapping.ColumnName, out var schemaInfo))
            {
                var propertyInfo = mapping.Property != null ? $"from property '{mapping.Property.Name}' " : "";
                throw new InvalidOperationException(
                    $"Column '{mapping.ColumnName}' {propertyInfo}not found in table schema. " +
                    $"Available columns: {string.Join(", ", schemaColumnNames)}");
            }

            matchedNames.Add(mapping.ColumnName);
            matchedTypes.Add(schemaInfo.Type);

            matchedGetters.Add(CreateGetter(mapping.Property!));

            // Try to create a typed extractor for direct-to-buffer writing
            try
            {
                var extractor = ColumnExtractorFactory.Create<T>(
                    mapping.Property!,
                    mapping.ColumnName,
                    schemaInfo.Type);
                matchedExtractors.Add(extractor);
            }
            catch (NotSupportedException)
            {
                // Type not supported for direct path (e.g., arrays, maps, tuples)
                canUseDirectPath = false;
            }
        }

        _columnNames = matchedNames.ToArray();
        _columnTypes = matchedTypes.ToArray();
        _getters = matchedGetters.ToArray();

        if (canUseDirectPath && matchedExtractors.Count == matchedNames.Count)
        {
            _extractors = matchedExtractors.ToArray();
            _useDirectPath = true;
        }
    }

    private static Func<T, object?> CreateGetter(PropertyInfo property)
    {
        // Create a compiled expression for fast property access
        var parameter = Expression.Parameter(typeof(T), "obj");
        var propertyAccess = Expression.Property(parameter, property);
        var convert = Expression.Convert(propertyAccess, typeof(object));
        var lambda = Expression.Lambda<Func<T, object?>>(convert, parameter);
        return lambda.Compile();
    }

    private object?[][] ExtractColumnData()
    {
        var columnCount = _columnNames!.Length;
        var rowCount = _buffer.Count;

        // Use pooled arrays to reduce GC pressure
        EnsurePooledArrays(columnCount, rowCount);

        // Extract values row by row using reflection-based getters
        for (int row = 0; row < rowCount; row++)
        {
            var item = _buffer[row];
            for (int col = 0; col < columnCount; col++)
            {
                _pooledColumnData![col][row] = _getters![col](item);
            }
        }

        return _pooledColumnData!;
    }

    private void EnsurePooledArrays(int columnCount, int rowCount)
    {
        // Check if we need to (re)allocate
        if (_pooledColumnData == null ||
            _pooledColumnData.Length < columnCount ||
            _pooledArraySize < rowCount)
        {
            // Return existing arrays to pool
            ReturnPooledArrays();

            // Rent new arrays from ArrayPool
            // Use BatchSize as the minimum to avoid frequent re-rentals
            var arraySize = Math.Max(rowCount, _options.BatchSize);
            _pooledColumnData = new object?[columnCount][];

            for (int col = 0; col < columnCount; col++)
            {
                _pooledColumnData[col] = ArrayPool<object?>.Shared.Rent(arraySize);
            }

            _pooledArraySize = arraySize;
        }
        else
        {
            // Clear only the portion we'll use (important for nullable correctness)
            for (int col = 0; col < columnCount; col++)
            {
                Array.Clear(_pooledColumnData[col], 0, rowCount);
            }
        }
    }

    private void ReturnPooledArrays()
    {
        if (_pooledColumnData != null)
        {
            for (int col = 0; col < _pooledColumnData.Length; col++)
            {
                if (_pooledColumnData[col] != null)
                {
                    ArrayPool<object?>.Shared.Return(_pooledColumnData[col], clearArray: true);
                }
            }
            _pooledColumnData = null;
            _pooledArraySize = 0;
        }
    }

    private sealed class PropertyMapping
    {
        public PropertyInfo? Property { get; init; }
        public required string ColumnName { get; init; }
        public int Order { get; init; }
    }
}
