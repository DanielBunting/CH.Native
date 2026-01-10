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
    public override void Cancel()
    {
        // Only attempt cancel if connection is open
        if (_connection?.State == ConnectionState.Open)
        {
            _connection.Inner.CancelCurrentQueryAsync().GetAwaiter().GetResult();
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

        return (int)await _connection!.Inner.ExecuteNonQueryWithParametersAsync(
            _commandText,
            nativeParams,
            progress: null,
            token).ConfigureAwait(false);
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

        return await _connection!.Inner.ExecuteScalarWithParametersAsync<object?>(
            _commandText,
            nativeParams,
            progress: null,
            token).ConfigureAwait(false);
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
        using var timeoutCts = CreateTimeoutCts(cancellationToken);
        var token = timeoutCts?.Token ?? cancellationToken;

        var reader = await _connection!.Inner.ExecuteReaderWithParametersAsync(
            _commandText,
            nativeParams,
            token).ConfigureAwait(false);
        return new ClickHouseDbDataReader(reader);
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
    private ClickHouseParameterCollection BuildNativeParameters()
    {
        var nativeParams = new ClickHouseParameterCollection();
        foreach (ClickHouseDbParameter p in _parameters)
        {
            if (!string.IsNullOrEmpty(p.ClickHouseType))
                nativeParams.Add(p.ParameterName, p.Value, p.ClickHouseType);
            else
                nativeParams.Add(p.ParameterName, p.Value);
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
