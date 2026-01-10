using System.Data;
using System.Data.Common;
using CH.Native.Connection;

namespace CH.Native.Ado;

/// <summary>
/// A ClickHouse-specific implementation of <see cref="DbConnection"/>.
/// Provides ADO.NET compatibility for use with Dapper and other ADO.NET-based libraries.
/// </summary>
/// <remarks>
/// <para>
/// <strong>Dapper Compatibility:</strong> This connection works with Dapper for scalar parameters
/// (int, string, DateTime, Guid, etc.). However, array parameters via Dapper's anonymous object
/// syntax are NOT supported due to Dapper's inline expansion behavior.
/// </para>
/// <para>
/// When Dapper sees an array in an anonymous object, it performs inline expansion for SQL IN clause
/// compatibility, converting <c>@ids</c> with <c>ids = [1, 2, 3]</c> into <c>(@ids0, @ids1, @ids2)</c>,
/// which creates a Tuple instead of a ClickHouse Array.
/// </para>
/// <para>
/// <strong>Workarounds for array parameters:</strong>
/// </para>
/// <para>
/// 1. Use direct ADO.NET:
/// <code>
/// using var cmd = connection.CreateCommand();
/// cmd.CommandText = "SELECT count() FROM t WHERE hasAny([id], @ids)";
/// cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "ids", Value = new[] { 1, 2, 3 } });
/// var result = await cmd.ExecuteScalarAsync();
/// </code>
/// </para>
/// <para>
/// 2. Use the native CH.Native API via the <see cref="Inner"/> property:
/// <code>
/// await connection.Inner.ExecuteScalarAsync&lt;long&gt;(
///     "SELECT count() FROM t WHERE hasAny([id], @ids)",
///     new ClickHouseParameterCollection { { "ids", new[] { 1, 2, 3 } } });
/// </code>
/// </para>
/// </remarks>
public sealed class ClickHouseDbConnection : DbConnection
{
    private ClickHouseConnection? _inner;
    private string _connectionString = "";
    private string? _currentDatabase;
    private ConnectionState _state = ConnectionState.Closed;

    /// <summary>
    /// Initializes a new instance of <see cref="ClickHouseDbConnection"/>.
    /// </summary>
    public ClickHouseDbConnection()
    {
    }

    /// <summary>
    /// Initializes a new instance of <see cref="ClickHouseDbConnection"/> with the specified connection string.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    public ClickHouseDbConnection(string connectionString)
    {
        _connectionString = connectionString ?? "";
    }

    /// <inheritdoc />
#pragma warning disable CS8765 // Nullability of parameter doesn't match overridden member
    public override string ConnectionString
    {
        get => _connectionString;
        set
        {
            if (_state != ConnectionState.Closed)
                throw new InvalidOperationException("Cannot change ConnectionString while connection is open.");
            _connectionString = value ?? "";
        }
    }
#pragma warning restore CS8765

    /// <inheritdoc />
    public override string Database => _currentDatabase ?? _inner?.Settings.Database ?? "";

    /// <inheritdoc />
    public override string DataSource => _inner != null
        ? $"{_inner.Settings.Host}:{_inner.Settings.EffectivePort}"
        : "";

    /// <inheritdoc />
    public override string ServerVersion => _inner?.ServerInfo != null
        ? $"{_inner.ServerInfo.VersionMajor}.{_inner.ServerInfo.VersionMinor}"
        : "";

    /// <inheritdoc />
    public override ConnectionState State => _state;

    /// <inheritdoc />
    public override void Open()
    {
        OpenAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        if (_state != ConnectionState.Closed)
            throw new InvalidOperationException("Connection is not closed.");

        _state = ConnectionState.Connecting;
        try
        {
            var settings = ClickHouseConnectionSettings.Parse(_connectionString);
            _inner = new ClickHouseConnection(settings);
            await _inner.OpenAsync(cancellationToken).ConfigureAwait(false);
            _currentDatabase = settings.Database;
            _state = ConnectionState.Open;
        }
        catch
        {
            _state = ConnectionState.Closed;
            _inner = null;
            throw;
        }
    }

    /// <inheritdoc />
    public override void Close()
    {
        CloseAsync().GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public override async Task CloseAsync()
    {
        if (_inner != null)
        {
            await _inner.CloseAsync().ConfigureAwait(false);
            _inner = null;
        }
        _state = ConnectionState.Closed;
        _currentDatabase = null;
    }

    /// <inheritdoc />
    public override void ChangeDatabase(string databaseName)
    {
        if (_state != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("Database name cannot be empty.", nameof(databaseName));

        // ClickHouse supports USE in native protocol
        // Escape backticks in database name
        using var cmd = CreateCommand();
        cmd.CommandText = $"USE `{databaseName.Replace("`", "``")}`";
        cmd.ExecuteNonQuery();
        _currentDatabase = databaseName;
    }

    /// <inheritdoc />
    protected override DbCommand CreateDbCommand()
    {
        return new ClickHouseDbCommand(this);
    }

    /// <inheritdoc />
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        throw new NotSupportedException(
            "ClickHouse does not support ACID transactions. " +
            "INSERTs are atomic per batch. For mutations, use ALTER TABLE...DELETE/UPDATE.");
    }

    /// <summary>
    /// Gets the underlying <see cref="ClickHouseConnection"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">Connection is not open.</exception>
    internal ClickHouseConnection Inner => _inner
        ?? throw new InvalidOperationException("Connection is not open.");

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();
        }
        base.Dispose(disposing);
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
        await base.DisposeAsync().ConfigureAwait(false);
    }
}
