using System.Collections;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Data.Variant;
using CH.Native.Protocol.Messages;
using CH.Native.Telemetry;

namespace CH.Native.Results;

/// <summary>
/// Forward-only stream of rows from a ClickHouse query result. Implements the
/// standard ADO.NET <see cref="DbDataReader"/> contract so Dapper, EF Core, and
/// other ADO consumers bind directly — no intermediate wrapper.
/// </summary>
/// <remarks>
/// <para>
/// Native callers should call <see cref="ReadAsync(System.Threading.CancellationToken)"/>
/// (which returns <see cref="ValueTask{Boolean}"/>) at least once before reading
/// schema properties like <see cref="FieldCount"/>; the strict
/// <see cref="EnsureInitialized"/> contract preserves the existing native
/// fast-fail behaviour.
/// </para>
/// <para>
/// ADO callers reaching the reader through <see cref="DbDataReader"/> may read
/// schema properties without a prior <c>Read</c>; in that case the schema is
/// primed via a synchronous <c>ReadAsync</c> dispatched through
/// <see cref="Task.Run(Func{Task})"/> so a captured single-threaded
/// <see cref="SynchronizationContext"/> cannot deadlock.
/// </para>
/// </remarks>
public sealed class ClickHouseDataReader : DbDataReader
{
    private readonly IAsyncEnumerator<object> _messageEnumerator;
    private readonly ClickHouseConnection? _connection;
    private readonly Activity? _activity;
    private readonly Stopwatch? _queryStopwatch;

    private TypedBlock? _currentBlock;
    private int _currentRowIndex = -1;
    private bool _hasInitialized;
    private bool _isCompleted;
    private bool _disposed;
    private long _rowsRead;
    private bool _failed;

    private ClickHouseColumn[]? _columns;
    private Dictionary<string, int>? _ordinalLookup;

    // ADO state. Populated by ClickHouseCommand.ExecuteDbDataReaderAsync via
    // AttachAdoLifetime so the timeout fires per-row (not just at query
    // dispatch) and CommandBehavior.CloseConnection closes the underlying
    // connection on dispose.
    private CancellationTokenSource? _timeoutCts;
    private ClickHouseConnection? _connectionToClose;

    // ADO lazy schema-priming state. ADO consumers may read FieldCount/HasRows
    // before any ReadAsync; the first ReadAsync is dispatched eagerly when
    // they do, and the first row is replayed on the next caller-driven
    // ReadAsync so the consumer doesn't lose it.
    private bool _adoPrimed;
    private bool _adoHasFirstRow;
    private bool _adoFirstRowConsumed;

    internal ClickHouseDataReader(
        IAsyncEnumerator<object> messageEnumerator,
        ClickHouseConnection? connection = null,
        Activity? activity = null,
        string? queryId = null,
        Stopwatch? queryStopwatch = null)
    {
        _messageEnumerator = messageEnumerator;
        _connection = connection;
        _activity = activity;
        _queryStopwatch = queryStopwatch;
        QueryId = queryId;
    }

    /// <summary>
    /// Attaches ADO lifetime state to this reader. Called by
    /// <c>ClickHouseCommand.ExecuteDbDataReaderAsync</c> after the reader is
    /// constructed via the native path so the CommandTimeout token survives
    /// across <see cref="ReadAsync(System.Threading.CancellationToken)"/> iterations
    /// and the <see cref="CommandBehavior.CloseConnection"/> flag closes the
    /// underlying connection on dispose.
    /// </summary>
    internal void AttachAdoLifetime(CancellationTokenSource? timeoutCts, ClickHouseConnection? connectionToClose)
    {
        _timeoutCts = timeoutCts;
        _connectionToClose = connectionToClose;
    }

    /// <summary>
    /// Gets the query ID for this reader's query. This reflects either the caller-supplied ID
    /// or the auto-generated GUID sent on the wire, matching the value in ClickHouse's
    /// <c>system.query_log</c>. Null if the reader was constructed without a query ID
    /// (legacy internal test paths only).
    /// </summary>
    public string? QueryId { get; }

    /// <summary>Number of columns in the result set.</summary>
    /// <exception cref="InvalidOperationException">Thrown if column metadata is not yet available.</exception>
    public override int FieldCount
    {
        get
        {
            EnsureInitializedForAdo();
            return _columns!.Length;
        }
    }

    /// <summary>True when the result set has at least one row.</summary>
    /// <exception cref="InvalidOperationException">Thrown if column metadata is not yet available.</exception>
    public override bool HasRows
    {
        get
        {
            EnsureInitializedForAdo();
            return _adoHasFirstRow || (_currentBlock is not null && _currentBlock.RowCount > 0);
        }
    }

    /// <summary>True after <c>Close</c>/<c>Dispose</c> has run.</summary>
    public override bool IsClosed => _disposed;

    /// <inheritdoc />
    public override int Depth => 0;

    /// <inheritdoc />
    public override int RecordsAffected => -1;

    /// <inheritdoc />
    public override object this[int ordinal] => GetValue(ordinal) ?? DBNull.Value;

    /// <inheritdoc />
    public override object this[string name] => GetValue(GetOrdinal(name)) ?? DBNull.Value;

    /// <summary>
    /// Gets the column metadata for all columns in the result set.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if column metadata is not yet available.</exception>
    public IReadOnlyList<ClickHouseColumn> Columns
    {
        get
        {
            EnsureInitialized();
            return _columns!;
        }
    }

    // Hot-path cached Tasks for the synchronous-completion fast path in the
    // Task<bool>-shaped DbDataReader.ReadAsync override. Avoids allocating a
    // new Task per row on buffered reads.
    private static readonly Task<bool> s_completedTrue = Task.FromResult(true);
    private static readonly Task<bool> s_completedFalse = Task.FromResult(false);

    /// <summary>
    /// Core row-advance path. Returns <see cref="ValueTask{Boolean}"/> so
    /// internal pipelines avoid a heap-allocated <see cref="Task"/> per row;
    /// the <see cref="ReadAsync(CancellationToken)"/> override wraps with a
    /// cached completed Task on the synchronous-completion fast path.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if there is another row; false if no more rows are available.</returns>
    private async ValueTask<bool> ReadCoreAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // First-row replay for ADO callers who hit FieldCount/HasRows before
        // any explicit Read. We already pulled the first block; hand it back
        // here once before continuing through MoveToNextBlockAsync.
        if (_adoPrimed && !_adoFirstRowConsumed)
        {
            _adoFirstRowConsumed = true;
            if (_adoHasFirstRow)
            {
                _rowsRead++;
                return true;
            }
            // ADO priming hit empty; fall through so the next Read drains.
        }

        bool advanced;
        try
        {
            if (!_hasInitialized)
            {
                await InitializeAsync(cancellationToken);
            }

            if (_isCompleted)
            {
                return false;
            }

            // Try to move to next row in current block
            _currentRowIndex++;

            if (_currentBlock is not null && _currentRowIndex < _currentBlock.RowCount)
            {
                advanced = true;
            }
            else
            {
                // Need to fetch next block
                advanced = await MoveToNextBlockAsync(cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _failed = true;
            // Streaming-query activity is owned by this reader, so error attribution
            // must happen here — the caller's catch never sees the activity.
            ClickHouseActivitySource.SetError(_activity, ex);
            throw;
        }

        if (advanced)
            _rowsRead++;

        return advanced;
    }

    /// <summary>
    /// Advances the reader to the next row. Bridges to <see cref="ReadCoreAsync"/>;
    /// pays a <see cref="Task"/> allocation only when the underlying call
    /// completes asynchronously (the buffered hot path returns a cached
    /// completed Task).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        var vt = ReadCoreAsync(cancellationToken);
        if (vt.IsCompletedSuccessfully)
            return vt.Result ? s_completedTrue : s_completedFalse;
        return vt.AsTask();
    }


    /// <summary>
    /// Synchronous Read. When there is no captured
    /// <see cref="SynchronizationContext"/> or non-default
    /// <see cref="TaskScheduler"/> we block on the async result directly;
    /// otherwise we hop to the thread pool via
    /// <see cref="Task.Run{TResult}(Func{Task{TResult}})"/> so a UI / classic
    /// ASP.NET caller cannot deadlock against the async continuation.
    /// </summary>
    public override bool Read()
    {
        if (SynchronizationContext.Current is null && TaskScheduler.Current == TaskScheduler.Default)
            return ReadAsync(CancellationToken.None).GetAwaiter().GetResult();
        return Task.Run(() => ReadAsync(CancellationToken.None)).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public override bool NextResult() => false;

    /// <inheritdoc />
    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        => Task.FromResult(false);

    /// <summary>
    /// Gets the value at the specified column ordinal.
    /// </summary>
    public override object GetValue(int ordinal)
    {
        EnsureCanRead();
        ValidateOrdinal(ordinal);
        return _currentBlock!.GetValue(_currentRowIndex, ordinal) ?? DBNull.Value;
    }

    /// <summary>
    /// Gets a typed value at the specified column ordinal.
    /// </summary>
    /// <typeparam name="T">The expected type.</typeparam>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The typed value at the specified column.</returns>
    public override T GetFieldValue<T>(int ordinal)
    {
        // Fast path: VariantValue<T0, T1> — skip the boxed ClickHouseVariant materialisation
        // and read directly from VariantTypedColumn arm columns.
        if (VariantValueDispatcher<T>.IsVariantValue)
        {
            EnsureCanRead();
            ValidateOrdinal(ordinal);
            return VariantValueDispatcher<T>.Read(_currentBlock!, _currentRowIndex, ordinal);
        }

        // Fast path: column storage matches the requested T exactly — read
        // straight from TypedColumn<T>'s typed indexer with no boxing. Cuts
        // ~24B/row for value-type columns on the Dapper-style row mapper hot
        // path (which calls GetXxx via GetFieldValue<T> per column per row).
        if (_currentBlock is not null && (uint)ordinal < (uint)_currentBlock.ColumnCount
            && _currentBlock.Columns[ordinal] is TypedColumn<T> typedColumn)
        {
            EnsureCanRead();
            return typedColumn[_currentRowIndex];
        }

        // DateTime64(8/9) columns keep their raw Int64 wire values — surface them for
        // callers that need the sub-tick digits a DateTime cannot represent. The value
        // is the unit count since epoch (toUnixTimestamp64Nano for precision 9).
        if (typeof(T) == typeof(long) && _currentBlock is not null
            && (uint)ordinal < (uint)_currentBlock.ColumnCount
            && _currentBlock.Columns[ordinal] is DateTime64RawColumn rawDateTime64)
        {
            EnsureCanRead();
            return (T)(object)rawDateTime64.GetRawValue(_currentRowIndex);
        }

        // Lazy-materialized String columns keep the raw, un-decoded bytes — surface them
        // for callers that need invalid-UTF-8-safe access. (Eager mode decodes during the
        // block read, so byte recovery requires StringMaterialization=Lazy.)
        if (typeof(T) == typeof(byte[]) && _currentBlock is not null
            && (uint)ordinal < (uint)_currentBlock.ColumnCount)
        {
            if (_currentBlock.Columns[ordinal] is RawStringColumn rawStrings)
            {
                EnsureCanRead();
                return (T)(object)rawStrings.GetBytesCopy(_currentRowIndex);
            }

            if (_currentBlock.Columns[ordinal] is NullableRawStringColumn nullableRawStrings)
            {
                EnsureCanRead();
                var bytes = nullableRawStrings.GetBytesCopy(_currentRowIndex);
                if (bytes is null)
                    return default!;
                return (T)(object)bytes;
            }
        }

        var value = GetValueInternal(ordinal);

        if (value is null)
        {
            if (default(T) is null)
                return default!;
            throw new InvalidCastException(
                $"Cannot convert null to non-nullable type {typeof(T).Name}");
        }

        if (value is T typed)
            return typed;

        // Handle DateTimeOffset → DateTime conversion for timezone-aware columns
        if (typeof(T) == typeof(DateTime) && value is DateTimeOffset dto)
            return (T)(object)dto.UtcDateTime;

        // JSON columns surface as JsonDocument. Caller asked for string → hand back
        // the raw text so GetFieldValue<string>("json_col") works as users expect
        // rather than falling through to Convert.ChangeType (JsonDocument doesn't
        // implement IConvertible and throws InvalidCastException).
        if (typeof(T) == typeof(string) && value is JsonDocument jsonDoc)
            return (T)(object)jsonDoc.RootElement.GetRawText();

        // Jagged → rectangular: column readers always materialize Array(Array(T))
        // as jagged. If the caller asks for T[,] (or higher rank), validate uniform
        // shape and copy into the rect form.
        if (typeof(T).IsArray && typeof(T).GetArrayRank() > 1 && value is Array jaggedArray)
            return (T)(object)Data.Conversion.JaggedToRectangularConverter.ToRectangular(jaggedArray, typeof(T));

        // Convert.ChangeType cannot target Nullable<U> directly — convert to the
        // underlying type and rely on the boxed-value-type → Nullable cast.
        var underlying = Nullable.GetUnderlyingType(typeof(T));
        if (underlying != null)
            return (T)Convert.ChangeType(value, underlying);

        // Handle numeric and other conversions
        return (T)Convert.ChangeType(value, typeof(T));
    }

    // GetValueInternal preserves the nullable shape for the typed-fallback
    // path inside GetFieldValue and for the ClickHouseRow indexer, which both
    // expose object? and surface SQL null as CLR null. The public
    // DbDataReader.GetValue contract returns object (non-nullable, DBNull for
    // SQL null), so it cannot be shared with those callers.
    internal object? GetValueInternal(int ordinal)
    {
        EnsureCanRead();
        ValidateOrdinal(ordinal);
        return _currentBlock!.GetValue(_currentRowIndex, ordinal);
    }

    /// <summary>
    /// Gets a typed value at the specified column name.
    /// </summary>
    public T GetFieldValue<T>(string name)
    {
        return GetFieldValue<T>(GetOrdinal(name));
    }

    /// <inheritdoc />
    public override bool GetBoolean(int ordinal) { ThrowIfClosed(); return GetFieldValue<bool>(ordinal); }

    /// <inheritdoc />
    public override byte GetByte(int ordinal) { ThrowIfClosed(); return GetFieldValue<byte>(ordinal); }

    /// <inheritdoc />
    public override char GetChar(int ordinal) { ThrowIfClosed(); return GetFieldValue<char>(ordinal); }

    /// <inheritdoc />
    public override DateTime GetDateTime(int ordinal) { ThrowIfClosed(); return GetFieldValue<DateTime>(ordinal); }

    /// <inheritdoc />
    public override decimal GetDecimal(int ordinal) { ThrowIfClosed(); return GetFieldValue<decimal>(ordinal); }

    /// <inheritdoc />
    public override double GetDouble(int ordinal) { ThrowIfClosed(); return GetFieldValue<double>(ordinal); }

    /// <inheritdoc />
    public override float GetFloat(int ordinal) { ThrowIfClosed(); return GetFieldValue<float>(ordinal); }

    /// <inheritdoc />
    public override Guid GetGuid(int ordinal) { ThrowIfClosed(); return GetFieldValue<Guid>(ordinal); }

    /// <inheritdoc />
    public override short GetInt16(int ordinal) { ThrowIfClosed(); return GetFieldValue<short>(ordinal); }

    /// <inheritdoc />
    public override int GetInt32(int ordinal) { ThrowIfClosed(); return GetFieldValue<int>(ordinal); }

    /// <inheritdoc />
    public override long GetInt64(int ordinal) { ThrowIfClosed(); return GetFieldValue<long>(ordinal); }

    /// <inheritdoc />
    public override string GetString(int ordinal) { ThrowIfClosed(); return GetFieldValue<string>(ordinal); }

    /// <inheritdoc />
    public override int GetValues(object[] values)
    {
        ThrowIfClosed();
        var count = Math.Min(values.Length, FieldCount);
        for (int i = 0; i < count; i++)
            values[i] = GetValue(i);
        return count;
    }

    /// <inheritdoc />
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        throw new NotSupportedException("GetBytes is not supported. Use GetFieldValue<byte[]>() instead.");
    }

    /// <inheritdoc />
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        throw new NotSupportedException("GetChars is not supported. Use GetString() instead.");
    }

    /// <summary>
    /// Gets the column ordinal for the specified column name.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown if the column name is not found.</exception>
    public override int GetOrdinal(string name)
    {
        EnsureInitializedForAdo();

        _ordinalLookup ??= BuildOrdinalLookup();

        if (_ordinalLookup.TryGetValue(name, out var ordinal))
            return ordinal;

        throw new ArgumentException($"Column '{name}' not found in result set.", nameof(name));
    }

    /// <summary>
    /// Gets the name of the column at the specified ordinal.
    /// </summary>
    public override string GetName(int ordinal)
    {
        EnsureInitializedForAdo();
        ValidateOrdinal(ordinal);
        return _columns![ordinal].Name;
    }

    /// <summary>
    /// Gets the ClickHouse type name of the column at the specified ordinal.
    /// </summary>
    public string GetTypeName(int ordinal)
    {
        EnsureInitializedForAdo();
        ValidateOrdinal(ordinal);
        return _columns![ordinal].ClickHouseTypeName;
    }

    /// <inheritdoc />
    public override string GetDataTypeName(int ordinal) => GetTypeName(ordinal);

    /// <summary>
    /// Gets the CLR type of the column at the specified ordinal.
    /// </summary>
    public override Type GetFieldType(int ordinal)
    {
        EnsureInitializedForAdo();
        ValidateOrdinal(ordinal);
        return _columns![ordinal].ClrType;
    }

    /// <summary>
    /// Checks if the value at the specified ordinal is null.
    /// </summary>
    public override bool IsDBNull(int ordinal)
    {
        EnsureCanRead();
        ValidateOrdinal(ordinal);
        // ITypedColumn.IsNull avoids the GetValue boxing for non-nullable
        // value-type storage — TypedColumn<long/double/DateTime/...>
        // short-circuits to `return false`.
        return _currentBlock!.Columns[ordinal].IsNull(_currentRowIndex);
    }

    /// <summary>
    /// Checks if the value at the specified column name is null.
    /// </summary>
    public bool IsDBNull(string name)
    {
        return IsDBNull(GetOrdinal(name));
    }

    /// <inheritdoc />
    public override IEnumerator GetEnumerator() => new DbEnumerator(this);

    /// <inheritdoc />
    public override DataTable GetSchemaTable()
    {
        EnsureInitializedForAdo();
        var table = new DataTable("SchemaTable");

        table.Columns.Add("ColumnName", typeof(string));
        table.Columns.Add("ColumnOrdinal", typeof(int));
        table.Columns.Add("DataType", typeof(Type));
        table.Columns.Add("ProviderType", typeof(string));
        table.Columns.Add("AllowDBNull", typeof(bool));

        for (int i = 0; i < FieldCount; i++)
        {
            var row = table.NewRow();
            row["ColumnName"] = GetName(i);
            row["ColumnOrdinal"] = i;
            row["DataType"] = GetFieldType(i);
            var typeName = GetDataTypeName(i);
            row["ProviderType"] = typeName;
            row["AllowDBNull"] = Data.Types.ClickHouseTypeParser.IsEffectivelyNullable(typeName);
            table.Rows.Add(row);
        }

        return table;
    }

    /// <summary>
    /// Synchronous Close. Dispatched via <see cref="Task.Run(Func{Task})"/>
    /// so a captured single-threaded <see cref="SynchronizationContext"/>
    /// (UI / classic ASP.NET) cannot deadlock against the async dispose.
    /// </summary>
    public override void Close()
    {
        Task.Run(() => CloseAsync()).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public override Task CloseAsync() => DisposeAsync().AsTask();

    /// <summary>
    /// Disposes the reader and drains any remaining server messages.
    /// If the query hasn't completed, sends a Cancel message to stop the server-side query.
    /// </summary>
    /// <summary>
    /// Marks the reader as having seen EndOfStream. Idempotent. Releases the
    /// connection's busy slot so a subsequent query on the same connection can
    /// proceed without waiting for explicit Dispose — natural enumerator
    /// completion is the contract.
    /// </summary>
    private void MarkCompleted()
    {
        if (_isCompleted) return;
        _isCompleted = true;
        // Owner-gated: after this resolves, a successor query may claim the slot
        // before the reader is disposed; the dispose safety-net's resolve then
        // no-ops instead of clobbering (or poisoning) the successor.
        _connection?.ExitBusyResolve(QueryId);
    }

    /// <summary>
    /// Disposes the reader and drains any remaining server messages so the underlying
    /// connection is left in a clean state. If the query hasn't completed, a Cancel
    /// message is sent to stop the server-side query first. Idempotent.
    /// </summary>
    /// <returns>A task that completes once the reader has been disposed.</returns>
    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        // If not completed, send cancel to server to stop the query
        if (!_isCompleted && _connection != null)
        {
            try
            {
                await _connection.SendCancelAsync();
            }
            catch
            {
                // Best effort - continue with disposal
            }
        }

        // Drain remaining messages to keep connection in clean state. The outer
        // try owns the cleanup finally; the inner try/catch swallows drain
        // failures so dispose never throws from the drain itself.
        try
        {
        try
        {
            while (!_isCompleted)
            {
                if (!await _messageEnumerator.MoveNextAsync())
                    break;

                if (_messageEnumerator.Current is EndOfStreamMessage)
                {
                    MarkCompleted();
                    break;
                }

                if (_messageEnumerator.Current is DataMessage drainedMessage)
                    drainedMessage.Block.Dispose();
            }
        }
        catch
        {
            // Don't throw from dispose
            _failed = true;
        }

        // The enumerator drain above is DEAD when the pump already faulted or
        // finished abnormally — most notably CommandTimeout: the pump enumerator
        // carries the timeout token, throws OCE, and its iterator is finished, so
        // MoveNextAsync above returns false immediately and drains NOTHING. The
        // abandoned response bytes would then be read by the NEXT query on this
        // connection as its own response (observed live: `SELECT 42` returning
        // the timed-out sleep query's 0). Fall back to the connection-level drain,
        // which reads the raw pipe to the response terminator (bounded, 30s) so
        // the wire is genuinely realigned — matching the scalar/non-query
        // timeout paths.
        // Gated on the connection's conversation evidence: only drain when bytes
        // went out AND no response terminator has been consumed. A server
        // exception, for instance, terminates the response (envelope consumed —
        // boundary proven) with the server back at idle; draining there would
        // read against a silent server until the 30s cap and wrongly poison.
        if (!_isCompleted
            && _connection is { IsOpen: true }
            && _connection.ConversationWrote
            && !_connection.BoundaryProven)
        {
            try
            {
                await _connection.DrainAfterCancellationAsync();
                // Wire is back at a boundary (or the drain marked it fatal).
                // MarkCompleted (not a raw flag) so the busy slot is released via
                // the standard completion path; the query metric still records
                // failure because ReadCoreAsync set _failed before propagating.
                MarkCompleted();
            }
            catch
            {
                // Best effort — the drain marks the connection protocol-fatal on
                // failure; nothing more to do from dispose.
                _failed = true;
            }
        }
        }
        finally
        {
            _currentBlock?.Dispose();
            _currentBlock = null;
            await _messageEnumerator.DisposeAsync();

            // Streaming reader owns the query lifetime, so the metric must be
            // recorded here rather than in ExecuteReaderAsync's caller — by the
            // time control returns there, rows haven't been consumed yet.
            if (_queryStopwatch is not null && _connection is not null)
            {
                _queryStopwatch.Stop();
                var success = _isCompleted && !_failed;
                ClickHouseMeter.RecordQuery(
                    _connection.Settings.Database,
                    _queryStopwatch.Elapsed,
                    _rowsRead,
                    success);
                if (!success)
                    ClickHouseMeter.ErrorsTotal.Add(1);
                if (success && QueryId is not null)
                    _connection.Logger.QueryCompleted(
                        QueryId,
                        _rowsRead,
                        _queryStopwatch.Elapsed.TotalMilliseconds);
            }

            _activity?.Dispose();

            // Safety net: resolve the conversation if natural completion didn't
            // fire (e.g. DisposeAsync called before any ReadAsync, or the drain
            // threw). Owner-gated on OUR QueryId: if MarkCompleted already
            // resolved and a successor query holds the slot, this no-ops rather
            // than releasing — or poisoning — the successor's conversation.
            _connection?.ExitBusyResolve(QueryId);

            // ADO lifetime hooks set by ClickHouseCommand.ExecuteDbDataReaderAsync.
            _timeoutCts?.Dispose();
            _timeoutCts = null;
            if (_connectionToClose is not null)
            {
                var conn = _connectionToClose;
                _connectionToClose = null;
                await conn.CloseAsync();
            }
        }
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();
        }
        base.Dispose(disposing);
    }

    private async ValueTask InitializeAsync(CancellationToken cancellationToken)
    {
        _hasInitialized = true;

        // Read first block to get schema
        while (await _messageEnumerator.MoveNextAsync())
        {
            cancellationToken.ThrowIfCancellationRequested();

            switch (_messageEnumerator.Current)
            {
                case DataMessage dataMessage:
                    InitializeColumnsFromBlock(dataMessage.Block);
                    _currentBlock = dataMessage.Block;
                    return;

                case EndOfStreamMessage:
                    MarkCompleted();
                    _columns = [];
                    return;

                case ProgressMessage:
                    // Skip progress messages
                    continue;
            }
        }

        // No messages received
        MarkCompleted();
        _columns = [];
    }

    private void InitializeColumnsFromBlock(TypedBlock block)
    {
        _columns = new ClickHouseColumn[block.ColumnCount];

        for (int i = 0; i < block.ColumnCount; i++)
        {
            // Determine CLR type from the typed column
            var clrType = block.Columns[i].ElementType;

            _columns[i] = new ClickHouseColumn(
                ordinal: i,
                name: block.ColumnNames[i],
                clickHouseTypeName: block.ColumnTypes[i],
                clrType: clrType);
        }
    }

    private async ValueTask<bool> MoveToNextBlockAsync(CancellationToken cancellationToken)
    {
        while (await _messageEnumerator.MoveNextAsync())
        {
            cancellationToken.ThrowIfCancellationRequested();

            switch (_messageEnumerator.Current)
            {
                case DataMessage dataMessage:
                    if (dataMessage.Block.RowCount > 0)
                    {
                        _currentBlock?.Dispose();
                        _currentBlock = dataMessage.Block;
                        _currentRowIndex = 0;
                        return true;
                    }
                    // Empty block — dispose and continue to next
                    dataMessage.Block.Dispose();
                    break;

                case EndOfStreamMessage:
                    MarkCompleted();
                    return false;

                case ProgressMessage:
                    // Skip progress messages
                    continue;
            }
        }

        MarkCompleted();
        return false;
    }

    private Dictionary<string, int> BuildOrdinalLookup()
    {
        var lookup = new Dictionary<string, int>(
            _columns!.Length,
            StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < _columns.Length; i++)
        {
            lookup[_columns[i].Name] = i;
        }

        return lookup;
    }

    /// <summary>
    /// Native priming contract: throws if no <see cref="ReadAsync(System.Threading.CancellationToken)"/>
    /// has run. Preserved so existing native callers continue to fast-fail.
    /// </summary>
    private void EnsureInitialized()
    {
        if (!_hasInitialized)
            throw new InvalidOperationException(
                "Reader has not been initialized. Call ReadAsync() first.");
    }

    /// <summary>
    /// ADO priming contract: lazily dispatches a first <c>ReadAsync</c> so
    /// schema-shaped property getters work for ADO consumers who haven't called
    /// <c>Read</c> yet. Dispatched via <see cref="Task.Run(Func{Task})"/> to
    /// escape any captured single-threaded synchronization context.
    /// </summary>
    private void EnsureInitializedForAdo()
    {
        if (_hasInitialized) return;
        _adoHasFirstRow = Task.Run(() => ReadCoreAsync(CancellationToken.None).AsTask()).GetAwaiter().GetResult();
        _adoPrimed = true;
    }

    private void EnsureCanRead()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_hasInitialized)
            throw new InvalidOperationException(
                "Reader has not been initialized. Call ReadAsync() first.");

        if (_currentRowIndex < 0)
            throw new InvalidOperationException(
                "No current row. Call ReadAsync() to advance to the first row.");

        if (_isCompleted)
            throw new InvalidOperationException(
                "Reader has finished reading all rows.");

        if (_currentBlock is null)
            throw new InvalidOperationException(
                "No data available.");
    }

    private void ThrowIfClosed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ClickHouseDataReader), "The DataReader has been closed.");
    }

    private void ValidateOrdinal(int ordinal)
    {
        var columns = _columns!;
        if (ordinal < 0 || ordinal >= columns.Length)
            throw new ArgumentOutOfRangeException(nameof(ordinal),
                $"Ordinal {ordinal} is out of range. Valid range: 0 to {columns.Length - 1}.");
    }
}

/// <summary>
/// Per-T cached dispatcher that detects <c>VariantValue&lt;T0, T1&gt;</c> and invokes
/// <see cref="TypedBlock.GetVariant{T0,T1}"/> via a closed delegate, avoiding reflection
/// on the hot read path.
/// </summary>
internal static class VariantValueDispatcher<T>
{
    public static readonly bool IsVariantValue;
    private static readonly Func<TypedBlock, int, int, T>? _reader;

    static VariantValueDispatcher()
    {
        var t = typeof(T);
        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(VariantValue<,>))
        {
            var args = t.GetGenericArguments();
            var method = typeof(TypedBlock)
                .GetMethod(nameof(TypedBlock.GetVariant), BindingFlags.Instance | BindingFlags.Public)!
                .MakeGenericMethod(args[0], args[1]);
            _reader = (Func<TypedBlock, int, int, T>)Delegate.CreateDelegate(
                typeof(Func<TypedBlock, int, int, T>), method);
            IsVariantValue = true;
        }
    }

    public static T Read(TypedBlock block, int row, int column) => _reader!(block, row, column);
}
