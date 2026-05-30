using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using CH.Native.Ado;
using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Mapping;
using CH.Native.Results;

namespace CH.Native.Commands;

/// <summary>
/// Represents a SQL command with parameters to execute against ClickHouse. Inherits
/// <see cref="DbCommand"/> so Dapper, EF Core, and other ADO.NET consumers bind
/// directly to instances created via <see cref="ClickHouseConnection.CreateCommand()"/>
/// or <see cref="ClickHouseConnection.CreateDbCommand"/>.
/// </summary>
/// <remarks>
/// <para>
/// Two parameter surfaces coexist on this type:
/// <see cref="Parameters"/> (native, strongly-typed <see cref="ClickHouseParameterCollection"/>)
/// and the protected <see cref="DbCommand.Parameters"/> override
/// (<see cref="ClickHouseDbParameterCollection"/> via the
/// <see cref="DbParameterCollection"/> override). They are independent
/// collections — native callers should populate <see cref="Parameters"/>; ADO
/// callers reaching the command through <see cref="DbCommand"/> populate the
/// ADO collection. At execute time both are merged into a single native
/// <see cref="ClickHouseParameterCollection"/> for the wire path.
/// </para>
/// <para>
/// Native execute methods (typed <see cref="ExecuteScalarAsync{T}"/>,
/// <see cref="ExecuteNonQueryAsync"/> with <see cref="IProgress{T}"/>,
/// <see cref="ExecuteReaderAsync"/>, <see cref="QueryStreamAsync"/>) are unaffected
/// by the DbCommand base and pass <see cref="Parameters"/> directly.
/// </para>
/// </remarks>
public sealed class ClickHouseCommand : DbCommand
{
    private ClickHouseConnection? _connection;
    private string _commandText = string.Empty;
    private CommandType _commandType = CommandType.Text;
    private List<string>? _roles;
    private ClickHouseDbParameterCollection? _adoParameters;

    /// <summary>
    /// Gets or sets the SQL command text.
    /// </summary>
#pragma warning disable CS8765 // Nullability of parameter doesn't match overridden member — DbCommand's contract is [AllowNull].
    public override string CommandText
    {
        get => _commandText;
        set => _commandText = value ?? string.Empty;
    }
#pragma warning restore CS8765

    /// <summary>
    /// Gets the native, strongly-typed parameter collection. Use this from
    /// native code paths; ADO consumers reaching the command through
    /// <see cref="DbCommand"/> see the protected
    /// <see cref="DbCommand.Parameters"/> override instead, which wraps a
    /// separate <see cref="ClickHouseDbParameterCollection"/>.
    /// </summary>
    public new ClickHouseParameterCollection Parameters { get; } = new();

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
    /// connection-level <see cref="ClickHouseConnectionSettings.Roles"/> when
    /// non-empty. Matches <c>ClickHouse.Driver</c>'s surface: mutable
    /// <see cref="IList{T}"/> so callers can use collection initialisers or
    /// append entries incrementally.
    /// </summary>
    /// <remarks>
    /// Empty list = "inherit the connection default" (parity with
    /// <c>ClickHouse.Driver</c>, whose <c>IList&lt;string&gt;</c> shape can't
    /// distinguish null from empty). For an explicit <c>SET ROLE NONE</c>, use
    /// <see cref="ClickHouseConnection.ChangeRolesAsync"/> with an empty array.
    /// Not thread-safe — mutate before <c>ExecuteXxxAsync</c> returns.
    /// </remarks>
    public IList<string> Roles => _roles ??= new List<string>();

    /// <summary>
    /// Gets or sets the query ID to send with this command. Set to override
    /// the auto-generated GUID; after execution the property reflects the ID
    /// that was actually sent on the wire (matching <c>system.query_log</c>).
    /// Null or empty means "generate a new GUID per execution". Maximum length
    /// is 128 characters.
    /// </summary>
    public string? QueryId { get; set; }

    /// <inheritdoc />
    protected override DbConnection? DbConnection
    {
        get => _connection;
        set => _connection = (ClickHouseConnection?)value;
    }

    /// <inheritdoc />
    protected override DbParameterCollection DbParameterCollection =>
        _adoParameters ??= new ClickHouseDbParameterCollection();

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

    /// <summary>
    /// Creates a disconnected command. Used by
    /// <see cref="System.Data.Common.DbProviderFactory.CreateCommand"/> and other
    /// ADO.NET factory patterns. You must assign <see cref="DbConnection"/>
    /// (or use a connected ctor) before calling any Execute method.
    /// </summary>
    public ClickHouseCommand()
    {
    }

    /// <summary>
    /// Creates a new command associated with the specified connection.
    /// </summary>
    /// <param name="connection">The connection to use for executing the command.</param>
    public ClickHouseCommand(ClickHouseConnection connection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    /// <summary>
    /// Creates a new command with the specified SQL text.
    /// </summary>
    /// <param name="connection">The connection to use for executing the command.</param>
    /// <param name="commandText">The SQL command text.</param>
    public ClickHouseCommand(ClickHouseConnection connection, string commandText)
        : this(connection)
    {
        CommandText = commandText;
    }

    /// <summary>
    /// Creates a new command with the specified SQL text and connection. ADO-style
    /// argument order (string-first) for compatibility with the previous
    /// <c>ClickHouseDbCommand</c> shape.
    /// </summary>
    /// <param name="commandText">The SQL command text.</param>
    /// <param name="connection">The connection to use for executing the command.</param>
    public ClickHouseCommand(string commandText, ClickHouseConnection connection)
        : this(connection, commandText)
    {
    }

    // ------------------------------------------------------------------
    // Native (strongly-typed) execute surface — unaffected by the
    // DbCommand promotion. Uses Parameters (native shape) directly.
    // ------------------------------------------------------------------

    /// <summary>
    /// Executes the command and returns the first column of the first row.
    /// </summary>
    /// <typeparam name="T">The expected result type.</typeparam>
    /// <param name="progress">Optional progress reporter.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The scalar result, or default if no rows returned.</returns>
    public Task<T?> ExecuteScalarAsync<T>(
        IProgress<QueryProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        EnsureConnection();
        return _connection!.ExecuteScalarWithParametersAsync<T>(
            CommandText, Parameters, progress, cancellationToken);
    }

    /// <summary>
    /// Executes the command that does not return rows.
    /// </summary>
    /// <param name="progress">Optional progress reporter.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of rows affected.</returns>
    public Task<long> ExecuteNonQueryAsync(
        IProgress<QueryProgress>? progress,
        CancellationToken cancellationToken = default)
    {
        EnsureConnection();
        return _connection!.ExecuteNonQueryWithParametersAsync(
            CommandText, Parameters, progress, cancellationToken);
    }

    /// <summary>
    /// Executes the command and returns a strongly-typed
    /// <see cref="ClickHouseDataReader"/>. Routes through the same
    /// ADO-shaped path as
    /// <see cref="DbCommand.ExecuteReaderAsync(System.Threading.CancellationToken)"/>
    /// so <see cref="CommandTimeout"/> and <see cref="CommandBehavior.CloseConnection"/>
    /// are honoured — the only difference is the strongly-typed return.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A data reader for iterating through results.</returns>
    public new async Task<ClickHouseDataReader> ExecuteReaderAsync(
        CancellationToken cancellationToken = default)
    {
        return (ClickHouseDataReader)await ExecuteDbDataReaderAsync(
            CommandBehavior.Default, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Streams the command's result as an async enumerable of rows.
    /// Renamed from <c>QueryAsync</c> in Phase 2 for naming parity with
    /// <see cref="Connection.ClickHouseConnection.QueryStreamAsync(string, CancellationToken, string?)"/>.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of rows.</returns>
    public async IAsyncEnumerable<ClickHouseRow> QueryStreamAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var reader = await ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            yield return new ClickHouseRow(reader);
        }
    }

    /// <summary>
    /// Streams the command's result as an async enumerable of mapped objects.
    /// Renamed from <c>QueryAsync</c> in Phase 2 for naming parity.
    /// </summary>
    /// <typeparam name="T">The type to map rows to. Must have a parameterless constructor.</typeparam>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of mapped objects.</returns>
    public async IAsyncEnumerable<T> QueryStreamAsync<T>(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var reader = await ExecuteReaderAsync(cancellationToken);

        // Need to call ReadAsync at least once to initialize schema before creating mapper
        if (!await reader.ReadAsync(cancellationToken))
            yield break;

        // Use reflection-based TypeMapper
        var mapper = new TypeMapper<T>(reader);

        // Map the first row
        yield return mapper.Map(reader);

        // Map remaining rows
        while (await reader.ReadAsync(cancellationToken))
        {
            yield return mapper.Map(reader);
        }
    }

    // ------------------------------------------------------------------
    // DbCommand overrides — ADO surface. These merge the native
    // Parameters collection with any ADO-collected parameters before
    // dispatching to the connection's With-Parameters paths.
    // ------------------------------------------------------------------

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
            Task.Run(() => conn.CancelCurrentQueryAsync()).GetAwaiter().GetResult();
        }
        catch
        {
            // Best-effort per System.Data.Common.DbCommand.Cancel contract.
        }
    }

    /// <inheritdoc />
    public override void Prepare()
    {
        // ClickHouse doesn't support server-side prepared statements; no-op for compatibility.
    }

    /// <inheritdoc />
    protected override DbParameter CreateDbParameter() => new ClickHouseDbParameter();

    /// <inheritdoc />
    /// <remarks>
    /// Dispatched via <see cref="Task.Run{TResult}(Func{Task{TResult}})"/> so a
    /// captured single-threaded <see cref="SynchronizationContext"/> (UI /
    /// classic ASP.NET) cannot deadlock against the async continuation. Async
    /// callers should prefer <see cref="ExecuteNonQueryAsync(CancellationToken)"/>.
    /// </remarks>
    public override int ExecuteNonQuery()
    {
        return Task.Run(() => ExecuteNonQueryAsync(CancellationToken.None)).GetAwaiter().GetResult();
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
            return (int)await _connection!.ExecuteNonQueryWithParametersAsync(
                CommandText,
                nativeParams,
                progress: null,
                token,
                rolesOverride: _roles is { Count: > 0 } ? _roles : null,
                queryId: QueryId).ConfigureAwait(false);
        }
        finally
        {
            QueryId = _connection!.LastQueryId ?? QueryId;
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Dispatched via <see cref="Task.Run{TResult}(Func{Task{TResult}})"/> so a
    /// captured single-threaded <see cref="SynchronizationContext"/> (UI /
    /// classic ASP.NET) cannot deadlock against the async continuation. Async
    /// callers should prefer <see cref="ExecuteScalarAsync(CancellationToken)"/>.
    /// </remarks>
    public override object? ExecuteScalar()
    {
        return Task.Run(() => ExecuteScalarAsync(CancellationToken.None)).GetAwaiter().GetResult();
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
            return await _connection!.ExecuteScalarWithParametersAsync<object?>(
                CommandText,
                nativeParams,
                progress: null,
                token,
                rolesOverride: _roles is { Count: > 0 } ? _roles : null,
                queryId: QueryId).ConfigureAwait(false);
        }
        finally
        {
            QueryId = _connection!.LastQueryId ?? QueryId;
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Dispatched via <see cref="Task.Run{TResult}(Func{Task{TResult}})"/> so a
    /// captured single-threaded <see cref="SynchronizationContext"/> (UI /
    /// classic ASP.NET) cannot deadlock against the async continuation. Async
    /// callers should prefer
    /// <see cref="DbCommand.ExecuteReaderAsync(CommandBehavior, CancellationToken)"/>.
    /// </remarks>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        return Task.Run(() => ExecuteDbDataReaderAsync(behavior, CancellationToken.None))
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
        // timer. Ownership of the CTS transfers to the returned ClickHouseDataReader.
        var timeoutCts = CreateTimeoutCts(cancellationToken);
        var token = timeoutCts?.Token ?? cancellationToken;

        try
        {
            var reader = await _connection!.ExecuteReaderWithParametersAsync(
                CommandText,
                nativeParams,
                token,
                rolesOverride: _roles is { Count: > 0 } ? _roles : null,
                queryId: QueryId).ConfigureAwait(false);
            QueryId = reader.QueryId ?? QueryId;

            // ADO contract: CommandBehavior.CloseConnection means "close the
            // owning DbConnection when this reader is disposed". Attach both
            // the timeout CTS and the connection-to-close to the native reader.
            var connectionToClose = (behavior & CommandBehavior.CloseConnection) != 0
                ? _connection
                : null;
            reader.AttachAdoLifetime(timeoutCts, connectionToClose);
            return reader;
        }
        catch
        {
            timeoutCts?.Dispose();
            throw;
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private void EnsureConnection()
    {
        if (_connection == null)
            throw new InvalidOperationException("Connection not set.");
        if (_connection.State != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");
    }

    /// <summary>
    /// Merges native <see cref="Parameters"/> with any ADO-supplied parameters
    /// (via the protected <see cref="DbCommand.Parameters"/> collection) into a
    /// single native collection for the wire path. Converts
    /// <see cref="DBNull"/>.<see cref="DBNull.Value"/> → <c>null</c> at the
    /// ADO boundary so type inference doesn't choke on DBNull.
    /// </summary>
    /// <remarks>
    /// When no ADO parameters are present the native collection passes through
    /// unchanged (no allocation). When both are populated, ADO names take
    /// precedence on collision so the ADO contract — "the parameter list you
    /// set is the one that runs" — is preserved.
    /// </remarks>
    private ClickHouseParameterCollection BuildNativeParameters()
    {
        if (_adoParameters is null || _adoParameters.Count == 0)
            return Parameters;

        var merged = new ClickHouseParameterCollection();
        foreach (var p in Parameters)
        {
            merged.Add(p);
        }
        foreach (ClickHouseDbParameter p in _adoParameters)
        {
            var value = p.Value is DBNull ? null : p.Value;
            if (!string.IsNullOrEmpty(p.ClickHouseType))
                merged.Add(p.ParameterName, value, p.ClickHouseType);
            else
                merged.Add(p.ParameterName, value);
        }
        return merged;
    }

    /// <summary>
    /// Creates a timeout CancellationTokenSource if <see cref="CommandTimeout"/>
    /// is positive. Linked to the caller's token so caller-cancellation still
    /// observes the same cancellation propagation.
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
