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

        // Initialize retry policy if configured
        if (settings.Resilience?.HasRetry == true)
        {
            _retryPolicy = new RetryPolicy(settings.Resilience.Retry);
        }

        // Initialize circuit breakers for each server if configured
        if (settings.Resilience?.HasCircuitBreaker == true)
        {
            foreach (var server in settings.Servers)
            {
                _circuitBreakers[server] = new CircuitBreaker(settings.Resilience.CircuitBreaker);
            }
        }

        // Initialize health checker and load balancer for multi-server configurations
        if (settings.Servers.Count > 1)
        {
            _healthChecker = new HealthChecker(
                settings.Servers,
                settings,
                settings.Resilience?.HealthCheckInterval);
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
    /// <param name="sql">The SQL command.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of affected rows.</returns>
    public async Task<long> ExecuteNonQueryAsync(
        string sql,
        CancellationToken cancellationToken = default)
    {
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
    /// The entire bulk insert operation is wrapped with resilience features.
    /// On transient failure, the entire insert will be retried.
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
        await ExecuteWithResilienceAsync(
            async ct =>
            {
                await EnsureConnectedAsync(ct).ConfigureAwait(false);
                await _currentConnection!.BulkInsertAsync(tableName, rows, options, ct).ConfigureAwait(false);
                return true;
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Bulk inserts rows from an async enumerable into the specified table.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Resilience features are applied to connection establishment and the overall operation.
    /// However, once streaming begins from the async enumerable, partial data may have been
    /// sent before a failure occurs. Consider using transactional semantics if atomicity is required.
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
            if (circuitBreaker != null)
            {
                connection = await circuitBreaker.ExecuteAsync(DoConnect, ct).ConfigureAwait(false);
            }
            else
            {
                connection = await DoConnect(ct).ConfigureAwait(false);
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

        try
        {
            if (_retryPolicy != null)
            {
                return await _retryPolicy.ExecuteAsync(operation, cancellationToken).ConfigureAwait(false);
            }
            return await operation(cancellationToken).ConfigureAwait(false);
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
        return ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(server.Host)
            .WithPort(server.Port)
            .WithDatabase(_settings.Database)
            .WithCredentials(_settings.Username, _settings.Password)
            .WithConnectTimeout(_settings.ConnectTimeout)
            .WithClientName(_settings.ClientName)
            .WithReceiveBufferSize(_settings.ReceiveBufferSize)
            .WithSendBufferSize(_settings.SendBufferSize)
            .WithCompression(_settings.Compress)
            .WithCompressionMethod(_settings.CompressionMethod)
            .Build();
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
