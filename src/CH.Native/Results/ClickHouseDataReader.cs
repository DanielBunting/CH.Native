using System.Collections.Concurrent;
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
/// Provides a way to read a forward-only stream of rows from a ClickHouse query result.
/// </summary>
public sealed class ClickHouseDataReader : IAsyncDisposable
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
    /// Gets the query ID for this reader's query. This reflects either the caller-supplied ID
    /// or the auto-generated GUID sent on the wire, matching the value in ClickHouse's
    /// <c>system.query_log</c>. Null if the reader was constructed without a query ID
    /// (legacy internal test paths only).
    /// </summary>
    public string? QueryId { get; }

    /// <summary>
    /// Gets the number of columns in the result set.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if column metadata is not yet available.</exception>
    public int FieldCount
    {
        get
        {
            EnsureInitialized();
            return _columns!.Length;
        }
    }

    /// <summary>
    /// Gets a value indicating whether the result set has any rows.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if column metadata is not yet available.</exception>
    public bool HasRows
    {
        get
        {
            EnsureInitialized();
            return _currentBlock is not null && _currentBlock.RowCount > 0;
        }
    }

    /// <summary>
    /// Gets a value indicating whether the reader is closed.
    /// </summary>
    public bool IsClosed => _disposed;

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

    /// <summary>
    /// Advances the reader to the next row.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if there is another row; false if no more rows are available.</returns>
    public async ValueTask<bool> ReadAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

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
        catch
        {
            _failed = true;
            throw;
        }

        if (advanced)
            _rowsRead++;

        return advanced;
    }

    /// <summary>
    /// Gets the value at the specified column ordinal.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value at the specified column.</returns>
    public object? GetValue(int ordinal)
    {
        EnsureCanRead();
        ValidateOrdinal(ordinal);
        return _currentBlock!.GetValue(_currentRowIndex, ordinal);
    }

    /// <summary>
    /// Gets a typed value at the specified column ordinal.
    /// </summary>
    /// <typeparam name="T">The expected type.</typeparam>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The typed value at the specified column.</returns>
    public T GetFieldValue<T>(int ordinal)
    {
        // Fast path: VariantValue<T0, T1> — skip the boxed ClickHouseVariant materialisation
        // and read directly from VariantTypedColumn arm columns.
        if (VariantValueDispatcher<T>.IsVariantValue)
        {
            EnsureCanRead();
            ValidateOrdinal(ordinal);
            return VariantValueDispatcher<T>.Read(_currentBlock!, _currentRowIndex, ordinal);
        }

        var value = GetValue(ordinal);

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

        // Convert.ChangeType cannot target Nullable<U> directly — convert to the
        // underlying type and rely on the boxed-value-type → Nullable cast.
        var underlying = Nullable.GetUnderlyingType(typeof(T));
        if (underlying != null)
            return (T)Convert.ChangeType(value, underlying);

        // Handle numeric and other conversions
        return (T)Convert.ChangeType(value, typeof(T));
    }

    /// <summary>
    /// Gets a typed value at the specified column name.
    /// </summary>
    /// <typeparam name="T">The expected type.</typeparam>
    /// <param name="name">The column name.</param>
    /// <returns>The typed value at the specified column.</returns>
    public T GetFieldValue<T>(string name)
    {
        return GetFieldValue<T>(GetOrdinal(name));
    }

    /// <summary>
    /// Gets the column ordinal for the specified column name.
    /// </summary>
    /// <param name="name">The column name (case-insensitive).</param>
    /// <returns>The zero-based column ordinal.</returns>
    /// <exception cref="ArgumentException">Thrown if the column name is not found.</exception>
    public int GetOrdinal(string name)
    {
        EnsureInitialized();

        _ordinalLookup ??= BuildOrdinalLookup();

        if (_ordinalLookup.TryGetValue(name, out var ordinal))
            return ordinal;

        throw new ArgumentException($"Column '{name}' not found in result set.", nameof(name));
    }

    /// <summary>
    /// Gets the name of the column at the specified ordinal.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The column name.</returns>
    public string GetName(int ordinal)
    {
        EnsureInitialized();
        ValidateOrdinal(ordinal);
        return _columns![ordinal].Name;
    }

    /// <summary>
    /// Gets the ClickHouse type name of the column at the specified ordinal.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The ClickHouse type name.</returns>
    public string GetTypeName(int ordinal)
    {
        EnsureInitialized();
        ValidateOrdinal(ordinal);
        return _columns![ordinal].ClickHouseTypeName;
    }

    /// <summary>
    /// Gets the CLR type of the column at the specified ordinal.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The CLR type.</returns>
    public Type GetFieldType(int ordinal)
    {
        EnsureInitialized();
        ValidateOrdinal(ordinal);
        return _columns![ordinal].ClrType;
    }

    /// <summary>
    /// Checks if the value at the specified ordinal is null.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>True if the value is null; otherwise false.</returns>
    public bool IsDBNull(int ordinal)
    {
        return GetValue(ordinal) is null;
    }

    /// <summary>
    /// Checks if the value at the specified column name is null.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <returns>True if the value is null; otherwise false.</returns>
    public bool IsDBNull(string name)
    {
        return IsDBNull(GetOrdinal(name));
    }

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
        _connection?.ExitBusy();
    }

    public async ValueTask DisposeAsync()
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

        // Drain remaining messages to keep connection in clean state
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

            // Safety net: release the connection's busy slot if natural
            // completion didn't fire (e.g. DisposeAsync called before any
            // ReadAsync, or drain threw). ExitBusy is idempotent.
            _connection?.ExitBusy();
        }
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

    private void EnsureInitialized()
    {
        if (!_hasInitialized)
            throw new InvalidOperationException(
                "Reader has not been initialized. Call ReadAsync() first.");
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
