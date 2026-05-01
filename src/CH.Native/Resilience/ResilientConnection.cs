using System.Runtime.CompilerServices;
using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Results;

namespace CH.Native.Resilience;

/// <summary>
/// A resilient ClickHouse connection that provides retry, circuit breaker, load balancing,
/// and health checking capabilities.
/// </summary>
/// <remarks>
/// <para>
/// <b>Session stickiness:</b> a single <c>ResilientConnection</c> instance is
/// not pinned to a single physical server. Each public call rents a healthy
/// endpoint via the <see cref="LoadBalancer"/>; on a transient failure the
/// retry path may select a different server for the next attempt. This is
/// fine for read-only SQL — <see cref="ExecuteScalarAsync{T}"/> and
/// <see cref="QueryAsync"/> are idempotent — and for write SQL the Round-3
/// fix in <see cref="ExecuteNonQueryAsync"/> short-circuits retry entirely,
/// so an INSERT cannot accidentally be replayed against a different replica.
/// Callers that genuinely need session-scoped state (temporary tables,
/// <c>SET ROLE</c>) should rent a raw <see cref="ClickHouseConnection"/>
/// from a <see cref="ClickHouseDataSource"/> instead.
/// </para>
/// </remarks>
public sealed class ResilientConnection : IAsyncDisposable
{
    private readonly ClickHouseConnectionSettings _settings;
    private readonly RetryPolicy? _retryPolicy;
    private readonly Dictionary<ServerAddress, CircuitBreaker> _circuitBreakers = new();
    private readonly HealthChecker? _healthChecker;
    private readonly LoadBalancer? _loadBalancer;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    private ClickHouseConnection? _currentConnection;
    private ServerAddress? _currentServer;
    private bool _disposed;

    /// <summary>
    /// Gets whether the connection is currently open.
    /// </summary>
    public bool IsOpen => _currentConnection?.IsOpen ?? false;

    /// <summary>
    /// Gets the current server address, if connected.
    /// </summary>
    public ServerAddress? CurrentServer => _currentServer;

    /// <summary>
    /// Gets the connection settings.
    /// </summary>
    public ClickHouseConnectionSettings Settings => _settings;

    /// <summary>
    /// Gets the number of healthy servers available.
    /// </summary>
    public int HealthyServerCount => _loadBalancer?.HealthyServerCount ?? (_settings.Servers.Count > 0 ? 1 : 0);

    /// <summary>
    /// Creates a new resilient connection with the specified settings.
    /// </summary>
    /// <param name="settings">The connection settings.</param>
    public ResilientConnection(ClickHouseConnectionSettings settings)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));

        var logger = new Telemetry.ClickHouseLogger(_settings.Telemetry?.LoggerFactory);

        // Initialize retry policy if configured
        if (settings.Resilience?.HasRetry == true)
        {
            _retryPolicy = new RetryPolicy(settings.Resilience.Retry, logger);
        }

        // Initialize circuit breakers for each server if configured
        if (settings.Resilience?.HasCircuitBreaker == true)
        {
            foreach (var server in settings.Servers)
            {
                _circuitBreakers[server] = new CircuitBreaker(settings.Resilience.CircuitBreaker, logger)
                {
                    ServerAddress = $"{server.Host}:{server.Port}"
                };
            }
        }

        // Initialize health checker and load balancer for multi-server configurations
        if (settings.Servers.Count > 1)
        {
            _healthChecker = new HealthChecker(
                settings.Servers,
                settings,
                settings.Resilience?.HealthCheckInterval,
                settings.Resilience?.HealthCheckTimeout,
                logger);
            _loadBalancer = new LoadBalancer(_healthChecker, settings.LoadBalancing);
        }
    }

    /// <summary>
    /// Creates a new resilient connection from a connection string.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    public ResilientConnection(string connectionString)
        : this(ClickHouseConnectionSettings.Parse(connectionString))
    {
    }

    /// <summary>
    /// Opens the connection.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task OpenAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_currentConnection?.IsOpen == true)
                return;

            await ConnectWithResilienceAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    /// <summary>
    /// Executes a scalar query and returns the first column of the first row.
    /// </summary>
    /// <typeparam name="T">The expected return type.</typeparam>
    /// <param name="sql">The SQL query.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The scalar result.</returns>
    public async Task<T?> ExecuteScalarAsync<T>(
        string sql,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteWithResilienceAsync(
            async ct =>
            {
                await EnsureConnectedAsync(ct).ConfigureAwait(false);
                return await _currentConnection!.ExecuteScalarAsync<T>(sql, cancellationToken: ct)
                    .ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a non-query command and returns the number of affected rows.
    /// </summary>
    /// <remarks>
    /// Resilience features (retry, circuit breaker, failover) are applied to
    /// the connect / first-attempt phase only when <paramref name="sql"/> is a
    /// write or DDL statement (INSERT, ALTER, OPTIMIZE, KILL, CREATE, DROP,
    /// RENAME, TRUNCATE, SET, GRANT, REVOKE, USE, SYSTEM, WITH-INSERT, etc.).
    /// Auto-retrying those after a transient error would risk duplicate rows
    /// or repeated side effects against ClickHouse, which has no general
    /// statement-level idempotency. SELECT-style reads continue to be retried.
    /// </remarks>
    /// <param name="sql">The SQL command.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of affected rows.</returns>
    public async Task<long> ExecuteNonQueryAsync(
        string sql,
        CancellationToken cancellationToken = default)
    {
        if (!SqlRetrySafety.IsRetrySafe(sql))
        {
            ThrowIfDisposed();
            await EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);
            return await _currentConnection!.ExecuteNonQueryAsync(sql, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }

        return await ExecuteWithResilienceAsync(
            async ct =>
            {
                await EnsureConnectedAsync(ct).ConfigureAwait(false);
                return await _currentConnection!.ExecuteNonQueryAsync(sql, cancellationToken: ct)
                    .ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a query and returns a data reader for streaming results.
    /// </summary>
    /// <remarks>
    /// Resilience features (retry, circuit breaker) are applied to connection establishment only.
    /// Once streaming begins, transient failures will propagate to the caller.
    /// </remarks>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A data reader for iterating through results.</returns>
    public async Task<ClickHouseDataReader> ExecuteReaderAsync(
        string sql,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);
        return await _currentConnection!.ExecuteReaderAsync(sql, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a query and returns an async enumerable of rows.
    /// </summary>
    /// <remarks>
    /// Resilience features (retry, circuit breaker) are applied to connection establishment only.
    /// Once streaming begins, transient failures will propagate to the caller.
    /// </remarks>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An async enumerable of rows.</returns>
    public async IAsyncEnumerable<ClickHouseRow> QueryAsync(
        string sql,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);

        await foreach (var row in _currentConnection!.QueryAsync(sql, cancellationToken).ConfigureAwait(false))
        {
            yield return row;
        }
    }

    /// <summary>
    /// Executes a query and returns an async enumerable of mapped objects.
    /// </summary>
    /// <remarks>
    /// Resilience features (retry, circuit breaker) are applied to connection establishment only.
    /// Once streaming begins, transient failures will propagate to the caller.
    /// </remarks>
    /// <typeparam name="T">The type to map rows to. Must have a parameterless constructor.</typeparam>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An async enumerable of mapped objects.</returns>
    public async IAsyncEnumerable<T> QueryAsync<T>(
        string sql,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : new()
    {
        ThrowIfDisposed();
        await EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);

        await foreach (var item in _currentConnection!.QueryAsync<T>(sql, cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    /// <summary>
    /// Bulk inserts rows into the specified table.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Resilience covers the connect phase only. Once the wire INSERT begins,
    /// no automatic retry is attempted: ClickHouse INSERTs into non-replicated
    /// tables are not idempotent, so a transparent retry after partial bytes
    /// were accepted would duplicate rows. This matches the behavior of the
    /// <see cref="IAsyncEnumerable{T}"/> overload below.
    /// </para>
    /// <para>
    /// If you need partial-commit semantics for very large inserts, split the
    /// source into smaller batches and issue a separate
    /// <c>BulkInsertAsync</c> per batch — each call is its own commit boundary.
    /// </para>
    /// </remarks>
    /// <typeparam name="T">The POCO type representing a row.</typeparam>
    /// <param name="tableName">The table name to insert into.</param>
    /// <param name="rows">The rows to insert.</param>
    /// <param name="options">Optional bulk insert options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task BulkInsertAsync<T>(
        string tableName,
        IEnumerable<T> rows,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        ThrowIfDisposed();
        await EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);
        await _currentConnection!.BulkInsertAsync(tableName, rows, options, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Bulk inserts rows from an async enumerable into the specified table.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A single <c>BulkInsertAsync</c> call corresponds to one ClickHouse native-protocol
    /// INSERT statement. The server only commits an INSERT when it receives the empty
    /// terminator block (sent internally by <c>BulkInserter.CompleteAsync</c>); blocks
    /// streamed beforehand are held server-side as uncommitted temporary parts. If the
    /// connection or the source enumerable fails mid-stream, the server cancels the
    /// in-progress sink and <strong>no rows are committed for that call</strong>.
    /// </para>
    /// <para>
    /// Resilience covers connection establishment only. Once enumeration begins, no
    /// retry is attempted, because rows already pulled from the <see cref="IAsyncEnumerable{T}"/>
    /// cannot be re-yielded. If you need partial-commit semantics, split the source
    /// into smaller chunks and issue a separate <c>BulkInsertAsync</c> call for each.
    /// </para>
    /// </remarks>
    /// <typeparam name="T">The POCO type representing a row.</typeparam>
    /// <param name="tableName">The table name to insert into.</param>
    /// <param name="rows">The async enumerable of rows to insert.</param>
    /// <param name="options">Optional bulk insert options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task BulkInsertAsync<T>(
        string tableName,
        IAsyncEnumerable<T> rows,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        ThrowIfDisposed();
        await EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);
        await _currentConnection!.BulkInsertAsync(tableName, rows, options, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets the underlying ClickHouseConnection. Use with caution - this bypasses resilience features.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The underlying connection.</returns>
    public async Task<ClickHouseConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);
        return _currentConnection!;
    }

    /// <summary>
    /// Closes the current connection.
    /// </summary>
    public async Task CloseAsync()
    {
        await _connectionLock.WaitAsync().ConfigureAwait(false);
        try
        {
            await CloseCurrentConnectionAsync().ConfigureAwait(false);
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private async Task ConnectWithResilienceAsync(CancellationToken cancellationToken)
    {
        async Task<bool> ConnectOperation(CancellationToken ct)
        {
            var server = GetNextAvailableServer();
            if (server == null)
            {
                throw new ClickHouseConnectionException("No healthy servers available")
                {
                    Host = _settings.Host,
                    Port = _settings.Port
                };
            }

            var circuitBreaker = _circuitBreakers.GetValueOrDefault(server.Value);

            async Task<ClickHouseConnection> DoConnect(CancellationToken innerCt)
            {
                var serverSettings = CreateSettingsForServer(server.Value);
                var connection = new ClickHouseConnection(serverSettings);
                await connection.OpenAsync(innerCt).ConfigureAwait(false);
                return connection;
            }

            ClickHouseConnection connection;
            try
            {
                if (circuitBreaker != null)
                {
                    connection = await circuitBreaker.ExecuteAsync(DoConnect, ct).ConfigureAwait(false);
                }
                else
                {
                    connection = await DoConnect(ct).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (RetryPolicy.IsTransientException(ex))
            {
                // Connect attempts that hit a transient failure are real, in-flight
                // observations of a bad server — take it out of rotation so the next
                // retry picks a different one. The background probe will restore it.
                _loadBalancer?.MarkServerFailed(server.Value);
                throw;
            }

            _currentConnection = connection;
            _currentServer = server;

            // Mark server as healthy on successful connection
            _loadBalancer?.MarkServerHealthy(server.Value);

            return true;
        }

        if (_retryPolicy != null)
        {
            await _retryPolicy.ExecuteAsync(ConnectOperation, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await ConnectOperation(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<T> ExecuteWithResilienceAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        // Track the previous attempt's exception so retries can decide whether the
        // wire is poisoned. ClickHouseConnection.IsOpen does not reflect peer-side
        // resets — _isOpen is only flipped to false in DisposeAsync — so we have
        // to evict the connection explicitly here. EnsureConnectedAsync (called
        // inside `operation`) then sees a null _currentConnection and reopens.
        Exception? lastException = null;

        async Task<T> WrappedOperation(CancellationToken ct)
        {
            if (lastException is not null && RetryPolicy.IsConnectionPoisoning(lastException))
            {
                await CloseCurrentConnectionAsync().ConfigureAwait(false);
            }

            try
            {
                return await operation(ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Tag the current server as failed each time we observe a transient
                // exception against it, not only when retries are exhausted. Without
                // this, a mid-flight failure that succeeds on retry would never
                // surface to the LB, leaving the dead server in rotation.
                if (_currentServer.HasValue && RetryPolicy.IsTransientException(ex))
                {
                    _loadBalancer?.MarkServerFailed(_currentServer.Value);
                }
                lastException = ex;
                throw;
            }
        }

        try
        {
            if (_retryPolicy != null)
            {
                return await _retryPolicy.ExecuteAsync(WrappedOperation, cancellationToken).ConfigureAwait(false);
            }
            return await WrappedOperation(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (RetryPolicy.IsTransientException(ex) && _currentServer.HasValue)
        {
            // Mark current server as potentially failed
            _loadBalancer?.MarkServerFailed(_currentServer.Value);

            // Close current connection to force reconnect on next operation
            await CloseCurrentConnectionAsync().ConfigureAwait(false);

            throw;
        }
    }

    private async Task EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        if (_currentConnection?.IsOpen != true)
        {
            await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_currentConnection?.IsOpen != true)
                {
                    await ConnectWithResilienceAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                _connectionLock.Release();
            }
        }
    }

    private ServerAddress? GetNextAvailableServer()
    {
        if (_loadBalancer != null)
        {
            return _loadBalancer.GetNextServer();
        }

        // Single server mode
        return _settings.Servers.FirstOrDefault();
    }

    private ClickHouseConnectionSettings CreateSettingsForServer(ServerAddress server)
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(server.Host)
            .WithPort(server.Port)
            .WithDatabase(_settings.Database)
            .WithUsername(_settings.Username)
            .WithConnectTimeout(_settings.ConnectTimeout)
            .WithClientName(_settings.ClientName)
            .WithReceiveBufferSize(_settings.ReceiveBufferSize)
            .WithSendBufferSize(_settings.SendBufferSize)
            .WithCompression(_settings.Compress)
            .WithCompressionMethod(_settings.CompressionMethod);

        if (_settings.UseTls)
        {
            builder.WithTls().WithTlsPort(_settings.TlsPort);
            if (_settings.AllowInsecureTls) builder.WithAllowInsecureTls();
            if (_settings.TlsCaCertificatePath is not null)
                builder.WithTlsCaCertificate(_settings.TlsCaCertificatePath);
            if (_settings.TlsClientCertificate is not null)
                builder.WithTlsClientCertificate(_settings.TlsClientCertificate);
        }

        if (_settings.Roles is not null)
        {
            builder.WithRoles(_settings.Roles);
        }

        switch (_settings.AuthMethod)
        {
            case ClickHouseAuthMethod.Jwt when _settings.JwtToken is not null:
                builder.WithJwt(_settings.JwtToken);
                break;
            case ClickHouseAuthMethod.SshKey when _settings.SshPrivateKey is not null:
                builder.WithSshKey(_settings.SshPrivateKey, _settings.SshPrivateKeyPassphrase);
                break;
            case ClickHouseAuthMethod.SshKey when _settings.SshPrivateKeyPath is not null:
                builder.WithSshKeyPath(_settings.SshPrivateKeyPath, _settings.SshPrivateKeyPassphrase);
                break;
            case ClickHouseAuthMethod.TlsClientCertificate:
                builder.WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate);
                break;
            default:
                builder.WithPassword(_settings.Password);
                break;
        }

        return builder.Build();
    }

    private async Task CloseCurrentConnectionAsync()
    {
        if (_currentConnection != null)
        {
            await _currentConnection.DisposeAsync().ConfigureAwait(false);
            _currentConnection = null;
            _currentServer = null;
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(ResilientConnection));
        }
    }

    /// <summary>
    /// Disposes the resilient connection and all associated resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        await CloseCurrentConnectionAsync().ConfigureAwait(false);

        if (_healthChecker != null)
        {
            await _healthChecker.DisposeAsync().ConfigureAwait(false);
        }

        _connectionLock.Dispose();
    }
}
