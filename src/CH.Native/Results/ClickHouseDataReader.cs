using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Protocol.Messages;

namespace CH.Native.Results;

/// <summary>
/// Provides a way to read a forward-only stream of rows from a ClickHouse query result.
/// </summary>
public sealed class ClickHouseDataReader : IAsyncDisposable
{
    private readonly IAsyncEnumerator<object> _messageEnumerator;
    private readonly ClickHouseConnection? _connection;
    private readonly Activity? _activity;

    private TypedBlock? _currentBlock;
    private int _currentRowIndex = -1;
    private bool _hasInitialized;
    private bool _isCompleted;
    private bool _disposed;

    private ClickHouseColumn[]? _columns;
    private Dictionary<string, int>? _ordinalLookup;

    internal ClickHouseDataReader(
        IAsyncEnumerator<object> messageEnumerator,
        ClickHouseConnection? connection = null,
        Activity? activity = null)
    {
        _messageEnumerator = messageEnumerator;
        _connection = connection;
        _activity = activity;
    }

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
            return true;
        }

        // Need to fetch next block
        return await MoveToNextBlockAsync(cancellationToken);
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
                    _isCompleted = true;
                    break;
                }
            }
        }
        catch
        {
            // Don't throw from dispose
        }
        finally
        {
            await _messageEnumerator.DisposeAsync();
            _activity?.Dispose();
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
                    _isCompleted = true;
                    _columns = [];
                    return;

                case ProgressMessage:
                    // Skip progress messages
                    continue;
            }
        }

        // No messages received
        _isCompleted = true;
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
                        _currentBlock = dataMessage.Block;
                        _currentRowIndex = 0;
                        return true;
                    }
                    // Empty block, continue to next
                    break;

                case EndOfStreamMessage:
                    _isCompleted = true;
                    return false;

                case ProgressMessage:
                    // Skip progress messages
                    continue;
            }
        }

        _isCompleted = true;
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
