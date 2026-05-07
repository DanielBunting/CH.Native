using System.Buffers;
using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Sql;
using CH.Native.Telemetry;

namespace CH.Native.BulkInsert;

/// <summary>
/// Provides high-performance bulk insert operations using the ClickHouse native protocol
/// without requiring a POCO type. Rows are supplied as <c>object?[]</c> arrays whose
/// element order matches the column-name list passed to the constructor.
/// </summary>
/// <remarks>
/// <para>
/// Use this when the row shape isn't a static POCO — runtime-defined columns, ETL
/// pipelines reading from <c>IDataReader</c>, dictionaries-of-values, etc. The
/// public lifecycle (<see cref="InitAsync"/> / <see cref="AddAsync"/> /
/// <see cref="FlushAsync"/> / <see cref="CompleteAsync"/>) mirrors
/// <see cref="BulkInserter{T}"/>; the connection-ownership, busy-slot and
/// cancellation-drain semantics are identical.
/// </para>
/// <para>
/// When <see cref="BulkInsertOptions.ColumnTypes"/> covers every column being
/// inserted, the server schema-probe round-trip is skipped and the schema is
/// built directly from the supplied dictionary. Mismatched types vs. the
/// server's actual schema surface as a <see cref="ClickHouseServerException"/>
/// at <see cref="CompleteAsync"/> time.
/// </para>
/// </remarks>
public sealed class DynamicBulkInserter : IAsyncDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly string _tableName;
    private readonly string _resolvedDatabase;
    private readonly string _resolvedTable;
    private readonly string _quotedQualifiedTable;
    private readonly string[] _columnNames;
    private readonly BulkInsertOptions _options;

    private string[]? _columnTypes;
    private object?[][]? _pooledColumnData;
    private int _pooledArraySize;
    private int _bufferedRows;

    private bool _initialized;
    private bool _completed;
    private bool _completeStarted;
    private bool _disposed;
    private bool _slotClaimed;
    private int _totalRowsInserted;
    private bool _usedCachedSchema;
    private bool _usedSuppliedColumnTypes;
    private SchemaKey _schemaCacheKey;
    private string? _effectiveQueryId;

    /// <summary>
    /// Creates a new dynamic bulk inserter for the specified table.
    /// </summary>
    /// <param name="connection">The ClickHouse connection to use.</param>
    /// <param name="tableName">
    /// The table name to insert into. May be qualified as <c>database.table</c>;
    /// in that case the rendered SQL addresses the named database directly
    /// (<c>`db`.`table`</c>). Tables whose name legitimately contains a dot
    /// must use the <c>(database, tableName, columnNames)</c> overload.
    /// </param>
    /// <param name="columnNames">The columns this inserter will write to, in the order rows will supply values.</param>
    /// <param name="options">Optional bulk insert options.</param>
#pragma warning disable RS0026, RS0027 // Sibling ctor of the (database, table) variant; both forms are intentional.
    public DynamicBulkInserter(
        ClickHouseConnection connection,
        string tableName,
        IReadOnlyList<string> columnNames,
        BulkInsertOptions? options = null)
#pragma warning restore RS0026, RS0027
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        ArgumentNullException.ThrowIfNull(columnNames);

        if (tableName.Length == 0)
            throw new ArgumentException("Table name must be non-empty.", nameof(tableName));
        if (columnNames.Count == 0)
            throw new ArgumentException("At least one column name is required.", nameof(columnNames));

        _columnNames = ValidateAndCopyColumnNames(columnNames);

        _options = options ?? BulkInsertOptions.Default;
        if (_options.BatchSize <= 0)
            throw new ArgumentOutOfRangeException(
                nameof(options),
                _options.BatchSize,
                $"{nameof(BulkInsertOptions)}.{nameof(BulkInsertOptions.BatchSize)} must be greater than zero.");

        var (db, table) = ClickHouseIdentifier.SplitQualifiedName(tableName, connection.Settings.Database);
        _resolvedDatabase = db;
        _resolvedTable = table;
        _quotedQualifiedTable = ClickHouseIdentifier.QuoteQualifiedName(tableName);
    }

    /// <summary>
    /// Creates a new dynamic bulk inserter targeting the explicitly-supplied
    /// <paramref name="database"/> and <paramref name="tableName"/>.
    /// </summary>
    /// <param name="connection">The ClickHouse connection to use.</param>
    /// <param name="database">The database segment. Quoted independently in the rendered SQL.</param>
    /// <param name="tableName">The table segment, used verbatim — dots in the name are not split.</param>
    /// <param name="columnNames">The columns this inserter will write to, in the order rows will supply values.</param>
    /// <param name="options">Optional bulk insert options.</param>
#pragma warning disable RS0026, RS0027 // Sibling ctor — distinct (database, table) parameter shape.
    public DynamicBulkInserter(
        ClickHouseConnection connection,
        string database,
        string tableName,
        IReadOnlyList<string> columnNames,
        BulkInsertOptions? options = null)
#pragma warning restore RS0026, RS0027
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(columnNames);

        if (database.Length == 0)
            throw new ArgumentException("Database must be non-empty.", nameof(database));
        if (tableName.Length == 0)
            throw new ArgumentException("Table name must be non-empty.", nameof(tableName));
        if (columnNames.Count == 0)
            throw new ArgumentException("At least one column name is required.", nameof(columnNames));

        _columnNames = ValidateAndCopyColumnNames(columnNames);

        _options = options ?? BulkInsertOptions.Default;
        if (_options.BatchSize <= 0)
            throw new ArgumentOutOfRangeException(
                nameof(options),
                _options.BatchSize,
                $"{nameof(BulkInsertOptions)}.{nameof(BulkInsertOptions.BatchSize)} must be greater than zero.");

        _resolvedDatabase = database;
        _resolvedTable = tableName;
        _tableName = $"{database}.{tableName}";
        _quotedQualifiedTable = ClickHouseIdentifier.Quote(database) + "." + ClickHouseIdentifier.Quote(tableName);
    }

    private static string[] ValidateAndCopyColumnNames(IReadOnlyList<string> columnNames)
    {
        var result = new string[columnNames.Count];
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < columnNames.Count; i++)
        {
            var name = columnNames[i];
            if (name is null)
                throw new ArgumentException($"Column name at index {i} is null.", nameof(columnNames));
            if (name.Length == 0)
                throw new ArgumentException($"Column name at index {i} is empty.", nameof(columnNames));
            if (!seen.Add(name))
                throw new ArgumentException(
                    $"Duplicate column name '{name}' in columnNames (case-insensitive).",
                    nameof(columnNames));
            result[i] = name;
        }
        return result;
    }

    /// <summary>Gets the number of rows currently buffered.</summary>
    public int BufferedCount => _bufferedRows;

    /// <summary>
    /// Suppresses the dispose-time "unflushed rows" failure for callers that
    /// already know the wire is being torn down (e.g. cancellation).
    /// </summary>
    internal void Abort()
    {
        _completeStarted = true;
        _bufferedRows = 0;
    }

    private ValueTask ObserveCancellationAsync(CancellationToken cancellationToken)
    {
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

    private CancellationTokenRegistration RegisterCancelHook(CancellationToken cancellationToken)
    {
        return cancellationToken.CanBeCanceled
            ? cancellationToken.Register(static state => _ = ((ClickHouseConnection)state!).SendCancelAsync(), _connection)
            : default;
    }

    private void ReleaseSlotIfClaimed()
    {
        if (!_slotClaimed) return;
        _slotClaimed = false;
        _connection.ExitBusy();
    }

    /// <summary>
    /// Initializes the bulk inserter by sending the INSERT query and resolving the schema.
    /// </summary>
    public async Task InitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_initialized)
            throw new InvalidOperationException("DynamicBulkInserter is already initialized.");

        cancellationToken.ThrowIfCancellationRequested();

        // Validate caller-supplied column types BEFORE any wire activity. Partial
        // coverage is a programming error and must surface without leaving the
        // wire in mid-INSERT state — otherwise the next operation on the
        // connection reads the orphaned schema block and reports a corrupt
        // protocol stream.
        string[]? suppliedTypes = null;
        var hasSuppliedTypes = TryBuildSchemaFromColumnTypes(out suppliedTypes);

        var effectiveQueryId = ClickHouseConnection.ResolveQueryIdInternal(_options.QueryId);
        _effectiveQueryId = effectiveQueryId;
        _connection.EnterBusyForBulkInsert(effectiveQueryId);
        _slotClaimed = true;

        using var registration = RegisterCancelHook(cancellationToken);
        try
        {
            var columnList = string.Join(", ", _columnNames.Select(ClickHouseIdentifier.Quote));
            _schemaCacheKey = new SchemaKey(_resolvedDatabase, _resolvedTable, columnList);

            var sql = $"INSERT INTO {_quotedQualifiedTable} ({columnList}) VALUES";
            var rolesSnapshot = _options.Roles is null ? null : (IReadOnlyList<string>)_options.Roles.ToArray();
            await _connection.SendInsertQueryAsync(
                sql,
                cancellationToken,
                rolesOverride: rolesSnapshot,
                queryId: effectiveQueryId);

            var useCache = _options.UseSchemaCache ?? _connection.Settings.UseSchemaCache;

            // Caller-supplied column types short-circuit the schema probe entirely.
            if (hasSuppliedTypes)
            {
                _columnTypes = suppliedTypes!;
                _usedSuppliedColumnTypes = true;
                _connection.Logger.BulkInsertSchemaFetched(_tableName, _columnNames.Length, fromCache: false);
            }
            else if (useCache && _connection.SchemaCache.TryGet(_schemaCacheKey, out var cachedSchema))
            {
                _columnTypes = MapColumnsToSchemaOrderedByCallSite(cachedSchema.ColumnNames, cachedSchema.ColumnTypes);
                _usedCachedSchema = true;
                _connection.Logger.BulkInsertSchemaFetched(_tableName, _columnNames.Length, fromCache: true);
            }
            else
            {
                var schemaBlock = await _connection.ReceiveSchemaBlockAsync(cancellationToken);
                _columnTypes = MapColumnsToSchemaOrderedByCallSite(schemaBlock.ColumnNames, schemaBlock.ColumnTypes);

                if (useCache)
                {
                    // Cache snapshot in caller order so downstream probes hit on the
                    // same column-list fingerprint regardless of the server's
                    // INSERT ordering.
                    _connection.SchemaCache.Set(
                        _schemaCacheKey,
                        new BulkInsertSchema((string[])_columnNames.Clone(), (string[])_columnTypes.Clone()));
                }

                _connection.Logger.BulkInsertSchemaFetched(_tableName, _columnNames.Length, fromCache: false);
            }

            _initialized = true;
        }
        catch (OperationCanceledException) when (_connection.WasCancellationRequested)
        {
            Abort();
            await _connection.DrainAfterCancellationAsync();
            ReleaseSlotIfClaimed();
            throw;
        }
        catch
        {
            _connection.ClearOwnedQueryId(effectiveQueryId);
            ReleaseSlotIfClaimed();
            throw;
        }
    }

    private bool TryBuildSchemaFromColumnTypes(out string[] types)
    {
        var supplied = _options.ColumnTypes;
        if (supplied is null || supplied.Count == 0)
        {
            types = Array.Empty<string>();
            return false;
        }

        // Normalise to OrdinalIgnoreCase if the caller supplied a different comparer.
        // We don't trust the caller's dictionary comparer because the contract on
        // the option specifies OrdinalIgnoreCase.
        var lookup = new Dictionary<string, string>(supplied.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var kv in supplied)
        {
            if (kv.Key is null)
                throw new InvalidOperationException(
                    "BulkInsertOptions.ColumnTypes contains a null key.");
            // Last-write-wins on case-insensitive collisions; the source dictionary
            // may have used a case-sensitive comparer and held both 'Foo' and 'foo'.
            lookup[kv.Key] = kv.Value;
        }

        var resolved = new string[_columnNames.Length];
        var missing = new List<string>();
        for (var i = 0; i < _columnNames.Length; i++)
        {
            if (!lookup.TryGetValue(_columnNames[i], out var type) || type is null)
            {
                missing.Add(_columnNames[i]);
                continue;
            }
            resolved[i] = type;
        }

        if (missing.Count == 0)
        {
            types = resolved;
            return true;
        }

        // Partial coverage is a programming error — bail loudly so the caller
        // doesn't silently fall through to the probe path on accident.
        throw new InvalidOperationException(
            $"BulkInsertOptions.ColumnTypes was supplied but does not cover every column in columnNames. " +
            $"Missing: {string.Join(", ", missing)}. Either supply types for every column or leave ColumnTypes null.");
    }

    private string[] MapColumnsToSchemaOrderedByCallSite(string[] schemaNames, string[] schemaTypes)
    {
        var lookup = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < schemaNames.Length; i++)
        {
            lookup[schemaNames[i]] = schemaTypes[i];
        }

        var resolved = new string[_columnNames.Length];
        for (var i = 0; i < _columnNames.Length; i++)
        {
            if (!lookup.TryGetValue(_columnNames[i], out var type))
            {
                throw new InvalidOperationException(
                    $"Column '{_columnNames[i]}' not found in table schema. " +
                    $"Available columns: {string.Join(", ", schemaNames)}");
            }
            resolved[i] = type;
        }
        return resolved;
    }

    /// <summary>
    /// Adds a single row to the buffer. Automatically flushes when batch size is reached.
    /// </summary>
    /// <param name="row">A row whose element count matches <c>columnNames</c> in the same order.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask AddAsync(object?[] row, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("DynamicBulkInserter must be initialized before adding rows.");
        if (_completed)
            throw new InvalidOperationException("DynamicBulkInserter has been completed and cannot accept more rows.");
        if (_completeStarted)
            throw new InvalidOperationException(
                "DynamicBulkInserter cannot accept more rows after a cancelled or failed CompleteAsync. " +
                "Create a new DynamicBulkInserter to retry.");

        ArgumentNullException.ThrowIfNull(row);
        if (row.Length != _columnNames.Length)
            throw new ArgumentException(
                $"Row arity mismatch: expected {_columnNames.Length} values to match column count, but got {row.Length}.",
                nameof(row));

        await ObserveCancellationAsync(cancellationToken).ConfigureAwait(false);

        EnsurePooledArrays();
        var rowIndex = _bufferedRows;
        for (var col = 0; col < _columnNames.Length; col++)
        {
            _pooledColumnData![col][rowIndex] = row[col];
        }
        _bufferedRows = rowIndex + 1;

        if (_bufferedRows >= _options.BatchSize)
        {
            await FlushAsync(cancellationToken);
        }
    }

#pragma warning disable RS0026, RS0027 // Sibling AddRangeAsync overloads — IEnumerable / IAsyncEnumerable.
    /// <summary>
    /// Adds multiple rows to the buffer.
    /// </summary>
    public async ValueTask AddRangeAsync(IEnumerable<object?[]> rows, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(rows);
        if (!_initialized)
            throw new InvalidOperationException("DynamicBulkInserter must be initialized before adding rows.");
        if (_completed)
            throw new InvalidOperationException("DynamicBulkInserter has been completed and cannot accept more rows.");
        if (_completeStarted)
            throw new InvalidOperationException(
                "DynamicBulkInserter cannot accept more rows after a cancelled or failed CompleteAsync. " +
                "Create a new DynamicBulkInserter to retry.");

        await ObserveCancellationAsync(cancellationToken).ConfigureAwait(false);

        foreach (var row in rows)
        {
            await AddAsync(row, cancellationToken);
        }
    }

    /// <summary>
    /// Adds multiple rows from an async enumerable.
    /// </summary>
    public async ValueTask AddRangeAsync(IAsyncEnumerable<object?[]> rows, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(rows);
        if (!_initialized)
            throw new InvalidOperationException("DynamicBulkInserter must be initialized before adding rows.");
        if (_completed)
            throw new InvalidOperationException("DynamicBulkInserter has been completed and cannot accept more rows.");
        if (_completeStarted)
            throw new InvalidOperationException(
                "DynamicBulkInserter cannot accept more rows after a cancelled or failed CompleteAsync. " +
                "Create a new DynamicBulkInserter to retry.");

        await ObserveCancellationAsync(cancellationToken).ConfigureAwait(false);

        await foreach (var row in rows.WithCancellation(cancellationToken))
        {
            await AddAsync(row, cancellationToken);
        }
    }
#pragma warning restore RS0026, RS0027

    /// <summary>
    /// Flushes the current buffer to the server.
    /// </summary>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("DynamicBulkInserter must be initialized before flushing.");

        await ObserveCancellationAsync(cancellationToken).ConfigureAwait(false);

        if (_bufferedRows == 0)
            return;

        using var activity = ClickHouseActivitySource.Source.StartActivity("clickhouse.bulk_insert.flush", ActivityKind.Client);
        if (activity != null)
        {
            activity.SetTag("db.system", "clickhouse");
            activity.SetTag("db.clickhouse.database", _resolvedDatabase);
            activity.SetTag("db.clickhouse.table", _resolvedTable);
            activity.SetTag("db.clickhouse.rows", _bufferedRows);
        }

        using var registration = RegisterCancelHook(cancellationToken);
        try
        {
            await _connection.SendDataBlockAsync(
                _columnNames,
                _columnTypes!,
                _pooledColumnData!,
                _bufferedRows,
                cancellationToken);

            _totalRowsInserted += _bufferedRows;
            _connection.Logger.BulkInsertFlushed(_tableName, _bufferedRows);

            ClickHouseMeter.RowsWrittenTotal.Add(
                _bufferedRows,
                new KeyValuePair<string, object?>("db.system", "clickhouse"),
                new KeyValuePair<string, object?>("db.name", _connection.Settings.Database),
                new KeyValuePair<string, object?>("db.clickhouse.database", _resolvedDatabase),
                new KeyValuePair<string, object?>("db.clickhouse.table", _resolvedTable));
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
            throw;
        }

        // Clear referenced row data eagerly so user objects can be GC'd before the
        // next flush window — matches the BulkInserter<T> per-row disposal cadence.
        for (var col = 0; col < _columnNames.Length; col++)
        {
            Array.Clear(_pooledColumnData![col], 0, _bufferedRows);
        }
        _bufferedRows = 0;
    }

    /// <summary>
    /// Completes the bulk insert: flushes any remaining rows and finalizes the wire.
    /// </summary>
    public async Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("DynamicBulkInserter must be initialized before completing.");
        if (_completed || _completeStarted)
            return;

        await ObserveCancellationAsync(cancellationToken).ConfigureAwait(false);

        _completeStarted = true;

        using var activity = ClickHouseActivitySource.StartBulkInsert(
            _resolvedTable,
            _connection.Settings.Database,
            _effectiveQueryId,
            _connection.Settings.Telemetry);
        activity?.SetTag("db.clickhouse.database", _resolvedDatabase);

        using var registration = RegisterCancelHook(cancellationToken);
        try
        {
            await FlushAsync(cancellationToken);
            await _connection.SendEmptyBlockAsync(cancellationToken);
            await _connection.ReceiveEndOfStreamAsync(cancellationToken);

            activity?.SetTag("db.clickhouse.rows", _totalRowsInserted);
            _completed = true;
            ReleaseSlotIfClaimed();
        }
        catch (ClickHouseServerException ex) when (_usedCachedSchema)
        {
            _connection.SchemaCache.InvalidateTable(_resolvedDatabase, _resolvedTable);
            ClickHouseActivitySource.SetError(activity, ex);
            throw;
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
            _connection.MarkProtocolFatal();
            throw;
        }
    }

    /// <summary>
    /// Disposes the bulk inserter. If buffered rows exist without an explicit
    /// <see cref="CompleteAsync"/>, an exception is thrown to surface the
    /// data-loss loudly.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        try
        {
            if (_initialized && !_completed && !_completeStarted)
            {
                if (_bufferedRows > 0)
                {
                    var bufferedCount = _bufferedRows;
                    var totalFlushedRows = _totalRowsInserted;
                    _bufferedRows = 0;
                    try { await _connection.SendEmptyBlockAsync(CancellationToken.None).ConfigureAwait(false); } catch { /* best-effort teardown */ }
                    try { await _connection.ReceiveEndOfStreamAsync(CancellationToken.None).ConfigureAwait(false); } catch { /* best-effort teardown */ }

                    throw new InvalidOperationException(
                        $"DynamicBulkInserter for table '{_tableName}' was disposed with {bufferedCount} " +
                        $"un-flushed row(s) and no call to CompleteAsync(). Those rows are LOST. " +
                        $"({totalFlushedRows} previously-flushed row(s) on this inserter ARE persisted — " +
                        $"ClickHouse commits each data block independently of CompleteAsync.) " +
                        $"Call CompleteAsync() explicitly before disposing to flush the buffer.");
                }

                await CompleteAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _disposed = true;
            ReturnPooledArrays();
            _bufferedRows = 0;
            ReleaseSlotIfClaimed();
        }
    }

    private void EnsurePooledArrays()
    {
        var columnCount = _columnNames.Length;
        var batchSize = _options.BatchSize;

        if (_pooledColumnData == null)
        {
            _pooledColumnData = new object?[columnCount][];
            for (var col = 0; col < columnCount; col++)
            {
                _pooledColumnData[col] = ArrayPool<object?>.Shared.Rent(batchSize);
            }
            _pooledArraySize = batchSize;
        }
        else if (_pooledArraySize < batchSize)
        {
            // Defensive — _options.BatchSize is read-only after ctor in practice,
            // but keep the resize path so a future mutable knob doesn't break us.
            ReturnPooledArrays();
            _pooledColumnData = new object?[columnCount][];
            for (var col = 0; col < columnCount; col++)
            {
                _pooledColumnData[col] = ArrayPool<object?>.Shared.Rent(batchSize);
            }
            _pooledArraySize = batchSize;
        }
    }

    private void ReturnPooledArrays()
    {
        if (_pooledColumnData != null)
        {
            for (var col = 0; col < _pooledColumnData.Length; col++)
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
}
