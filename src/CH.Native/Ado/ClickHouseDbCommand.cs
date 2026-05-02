using System.Data;
using System.Data.Common;
using CH.Native.Commands;

namespace CH.Native.Ado;

/// <summary>
/// A ClickHouse-specific implementation of <see cref="DbCommand"/>.
/// </summary>
public sealed class ClickHouseDbCommand : DbCommand
{
    private ClickHouseDbConnection? _connection;
    private readonly ClickHouseDbParameterCollection _parameters = new();
    private string _commandText = "";
    private CommandType _commandType = CommandType.Text;

    /// <summary>
    /// Initializes a new instance of <see cref="ClickHouseDbCommand"/>.
    /// </summary>
    public ClickHouseDbCommand()
    {
    }

    /// <summary>
    /// Initializes a new instance of <see cref="ClickHouseDbCommand"/> with the specified connection.
    /// </summary>
    /// <param name="connection">The connection to use.</param>
    public ClickHouseDbCommand(ClickHouseDbConnection connection)
    {
        _connection = connection;
    }

    /// <summary>
    /// Initializes a new instance of <see cref="ClickHouseDbCommand"/> with the specified SQL text and connection.
    /// </summary>
    /// <param name="commandText">The SQL command text.</param>
    /// <param name="connection">The connection to use.</param>
    public ClickHouseDbCommand(string commandText, ClickHouseDbConnection connection)
    {
        _commandText = commandText ?? "";
        _connection = connection;
    }

    /// <inheritdoc />
#pragma warning disable CS8765 // Nullability of parameter doesn't match overridden member
    public override string CommandText
    {
        get => _commandText;
        set => _commandText = value ?? "";
    }
#pragma warning restore CS8765

    /// <inheritdoc />
    public override int CommandTimeout { get; set; } = 30;

    /// <inheritdoc />
    public override CommandType CommandType
    {
        get => _commandType;
        set
        {
            if (value != CommandType.Text)
                throw new NotSupportedException("ClickHouse only supports CommandType.Text");
            _commandType = value;
        }
    }

    /// <inheritdoc />
    public override bool DesignTimeVisible { get; set; }

    /// <inheritdoc />
    public override UpdateRowSource UpdatedRowSource { get; set; }

    /// <summary>
    /// Gets the ClickHouse roles to activate for this command. Overrides the
    /// connection-level <see cref="Connection.ClickHouseConnectionSettings.Roles"/>
    /// when non-empty. Matches <c>ClickHouse.Driver</c>'s surface: mutable
    /// <see cref="IList{T}"/> so callers can use collection initialisers or append
    /// entries incrementally.
    /// </summary>
    /// <remarks>
    /// Empty list = "inherit the connection default" (parity with
    /// <c>ClickHouse.Driver</c>, whose <c>IList&lt;string&gt;</c> shape can't
    /// distinguish null from empty). For an explicit <c>SET ROLE NONE</c>, use
    /// <see cref="Connection.ClickHouseConnection.ChangeRolesAsync"/> with an
    /// empty array. Not thread-safe — mutate before <c>ExecuteXxxAsync</c> returns.
    /// </remarks>
    public IList<string> Roles => _roles ??= new List<string>();

    private List<string>? _roles;

    /// <summary>
    /// Gets or sets the query ID to send with this command. Set to override the auto-generated
    /// GUID; after execution the property reflects the ID that was actually sent on the wire
    /// (matching <c>system.query_log</c>). Null or empty means "generate a new GUID per execution".
    /// Maximum length is 128 characters.
    /// </summary>
    public string? QueryId { get; set; }

    /// <inheritdoc />
    protected override DbConnection? DbConnection
    {
        get => _connection;
        set => _connection = (ClickHouseDbConnection?)value;
    }

    /// <inheritdoc />
    protected override DbParameterCollection DbParameterCollection => _parameters;

    /// <inheritdoc />
    protected override DbTransaction? DbTransaction
    {
        get => null;
        set
        {
            if (value != null)
                throw new NotSupportedException(
                    "ClickHouse does not support transactions. " +
                    "INSERTs are atomic per batch. For mutations, use ALTER TABLE...DELETE/UPDATE.");
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// ADO contract: <c>Cancel()</c> is best-effort and must not throw on an
    /// inactive command or a broken connection. The synchronous wait is
    /// dispatched via <see cref="Task.Run(Func{Task})"/> so a captured
    /// single-threaded <see cref="SynchronizationContext"/> (UI / classic
    /// ASP.NET) cannot deadlock against the in-flight query's continuation,
    /// and any exception from <c>CancelCurrentQueryAsync</c> is swallowed.
    /// </remarks>
    public override void Cancel()
    {
        if (_connection?.State != ConnectionState.Open) return;
        var conn = _connection;
        try
        {
            Task.Run(() => conn.Inner.CancelCurrentQueryAsync()).GetAwaiter().GetResult();
        }
        catch
        {
            // Best-effort per System.Data.Common.DbCommand.Cancel contract.
        }
    }

    /// <inheritdoc />
    public override int ExecuteNonQuery()
    {
        return ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        EnsureConnection();
        var nativeParams = BuildNativeParameters();
        using var timeoutCts = CreateTimeoutCts(cancellationToken);
        var token = timeoutCts?.Token ?? cancellationToken;

        try
        {
            return (int)await _connection!.Inner.ExecuteNonQueryWithParametersAsync(
                _commandText,
                nativeParams,
                progress: null,
                token,
                rolesOverride: _roles is { Count: > 0 } ? _roles : null,
                queryId: QueryId).ConfigureAwait(false);
        }
        finally
        {
            QueryId = _connection!.Inner.LastQueryId ?? QueryId;
        }
    }

    /// <inheritdoc />
    public override object? ExecuteScalar()
    {
        return ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        EnsureConnection();
        var nativeParams = BuildNativeParameters();
        using var timeoutCts = CreateTimeoutCts(cancellationToken);
        var token = timeoutCts?.Token ?? cancellationToken;

        try
        {
            return await _connection!.Inner.ExecuteScalarWithParametersAsync<object?>(
                _commandText,
                nativeParams,
                progress: null,
                token,
                rolesOverride: _roles is { Count: > 0 } ? _roles : null,
                queryId: QueryId).ConfigureAwait(false);
        }
        finally
        {
            QueryId = _connection!.Inner.LastQueryId ?? QueryId;
        }
    }

    /// <inheritdoc />
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        return ExecuteDbDataReaderAsync(behavior, CancellationToken.None)
            .GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
        CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        EnsureConnection();
        var nativeParams = BuildNativeParameters();
        // CommandTimeout for readers must outlive this method — the reader is
        // returned to the caller and ReadAsync iterations need to observe the
        // timer. Ownership of the CTS transfers to the returned
        // ClickHouseDbDataReader, which disposes it when the reader is disposed.
        var timeoutCts = CreateTimeoutCts(cancellationToken);
        var token = timeoutCts?.Token ?? cancellationToken;

        try
        {
            var reader = await _connection!.Inner.ExecuteReaderWithParametersAsync(
                _commandText,
                nativeParams,
                token,
                rolesOverride: _roles is { Count: > 0 } ? _roles : null,
                queryId: QueryId).ConfigureAwait(false);
            QueryId = reader.QueryId ?? QueryId;

            // ADO contract: CommandBehavior.CloseConnection means "close the
            // owning DbConnection when this reader is disposed". The wrapper
            // reader honours this by holding the connection reference and
            // closing it on Dispose; if the flag is not set, the connection
            // outlives the reader as before.
            var connectionToClose = (behavior & CommandBehavior.CloseConnection) != 0
                ? _connection
                : null;
            return new ClickHouseDbDataReader(reader, timeoutCts, connectionToClose);
        }
        catch
        {
            timeoutCts?.Dispose();
            throw;
        }
    }

    /// <inheritdoc />
    public override void Prepare()
    {
        // ClickHouse doesn't support server-side prepared statements
        // This is a no-op for compatibility
    }

    /// <inheritdoc />
    protected override DbParameter CreateDbParameter()
    {
        return new ClickHouseDbParameter();
    }

    private void EnsureConnection()
    {
        if (_connection == null)
            throw new InvalidOperationException("Connection not set.");
        if (_connection.State != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");
    }

    /// <summary>
    /// Converts ADO.NET parameters to native ClickHouseParameterCollection.
    /// </summary>
    /// <remarks>
    /// ADO.NET callers commonly use <see cref="DBNull"/>.<see cref="DBNull.Value"/>
    /// to represent SQL NULL. The native layer expects plain <c>null</c>, so we
    /// translate at this boundary — without this, type inference would fail with
    /// a cryptic "Cannot infer type from DBNull" error instead of the documented
    /// "Cannot infer type from null" guidance to set <c>ClickHouseType</c> explicitly.
    /// </remarks>
    private ClickHouseParameterCollection BuildNativeParameters()
    {
        var nativeParams = new ClickHouseParameterCollection();
        foreach (ClickHouseDbParameter p in _parameters)
        {
            var value = p.Value is DBNull ? null : p.Value;
            if (!string.IsNullOrEmpty(p.ClickHouseType))
                nativeParams.Add(p.ParameterName, value, p.ClickHouseType);
            else
                nativeParams.Add(p.ParameterName, value);
        }
        return nativeParams;
    }

    /// <summary>
    /// Creates a timeout CancellationTokenSource if CommandTimeout is set.
    /// </summary>
    private CancellationTokenSource? CreateTimeoutCts(CancellationToken cancellationToken)
    {
        if (CommandTimeout <= 0)
            return null;

        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(CommandTimeout));
        return cts;
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
    }
}
