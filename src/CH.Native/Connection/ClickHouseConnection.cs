using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using CH.Native.BulkInsert;
using CH.Native.Commands;
using CH.Native.Compression;
using CH.Native.Data;
using CH.Native.Exceptions;
using CH.Native.Parameters;
using CH.Native.Protocol;
using CH.Native.Protocol.Messages;
using CH.Native.Results;
using CH.Native.Telemetry;

namespace CH.Native.Connection;

/// <summary>
/// Represents a connection to a ClickHouse server using the native protocol.
/// </summary>
public sealed class ClickHouseConnection : IAsyncDisposable
{
    private readonly ClickHouseConnectionSettings _settings;
    private readonly ClickHouseLogger _logger;
    private readonly object _queryLock = new();
    private TcpClient? _tcpClient;
    private Stream? _networkStream;
    private SslStream? _sslStream;
    private PipeReader? _pipeReader;
    private PipeWriter? _pipeWriter;
    private bool _isOpen;
    private bool _disposed;
    private bool _compressionEnabled;
    private string? _currentQueryId;
    private volatile bool _cancellationRequested;
    private X509Certificate2? _customCaCertificate;

    /// <summary>
    /// Gets the server information received during handshake.
    /// </summary>
    public ServerHello? ServerInfo { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the connection is open.
    /// </summary>
    public bool IsOpen => _isOpen && !_disposed;

    /// <summary>
    /// Gets the negotiated protocol version (minimum of client and server).
    /// </summary>
    public int NegotiatedProtocolVersion { get; private set; }

    /// <summary>
    /// Gets the connection settings.
    /// </summary>
    public ClickHouseConnectionSettings Settings => _settings;

    /// <summary>
    /// Creates a new connection using a connection string.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    public ClickHouseConnection(string connectionString)
        : this(ClickHouseConnectionSettings.Parse(connectionString))
    {
    }

    /// <summary>
    /// Creates a new connection using settings.
    /// </summary>
    /// <param name="settings">The connection settings.</param>
    public ClickHouseConnection(ClickHouseConnectionSettings settings)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _logger = new ClickHouseLogger(_settings.Telemetry?.LoggerFactory);
    }

    /// <summary>
    /// Opens the connection and performs the handshake.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task OpenAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_isOpen)
            throw new InvalidOperationException("Connection is already open.");

        using var activity = ClickHouseActivitySource.StartConnect(
            _settings.Host, _settings.EffectivePort, _settings.Telemetry);
        var stopwatch = Stopwatch.StartNew();

        try
        {
            await ConnectTcpAsync(cancellationToken);
            await PerformHandshakeAsync(cancellationToken);
            _isOpen = true;

            // Record telemetry on success
            stopwatch.Stop();
            ClickHouseActivitySource.SetServerInfo(activity, ServerInfo!);
            ClickHouseMeter.ConnectDuration.Record(stopwatch.Elapsed.TotalSeconds);
            ClickHouseMeter.IncrementConnections();
            _logger.ConnectionOpened(_settings.Host, _settings.EffectivePort,
                stopwatch.Elapsed.TotalMilliseconds, NegotiatedProtocolVersion);
        }
        catch (Exception ex)
        {
            ClickHouseActivitySource.SetError(activity, ex);
            _logger.ConnectionFailed(_settings.Host, _settings.EffectivePort, ex.Message);
            await CloseInternalAsync();
            throw;
        }
    }

    private async Task ConnectTcpAsync(CancellationToken cancellationToken)
    {
        _tcpClient = new TcpClient
        {
            ReceiveBufferSize = _settings.ReceiveBufferSize,
            SendBufferSize = _settings.SendBufferSize,
            NoDelay = true
        };

        // Use the effective port (TlsPort if TLS enabled, otherwise Port)
        var port = _settings.EffectivePort;

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_settings.ConnectTimeout);

        try
        {
            await _tcpClient.ConnectAsync(_settings.Host, port, timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw ClickHouseConnectionException.Timeout(_settings.Host, port, _settings.ConnectTimeout);
        }
        catch (SocketException ex)
        {
            throw ClickHouseConnectionException.Refused(_settings.Host, port, ex);
        }

        _networkStream = _tcpClient.GetStream();

        // Wrap with TLS if enabled
        if (_settings.UseTls)
        {
            _networkStream = await EstablishTlsAsync(_networkStream, timeoutCts.Token);
        }

        // Use larger buffer sizes to reduce fragmentation and improve IsSingleSegment hit rate
        var pipeReaderOptions = new StreamPipeReaderOptions(
            bufferSize: _settings.PipeBufferSize,
            minimumReadSize: _settings.PipeBufferSize / 4);
        _pipeReader = PipeReader.Create(_networkStream, pipeReaderOptions);
        _pipeWriter = PipeWriter.Create(_networkStream);
    }

    private async Task<Stream> EstablishTlsAsync(Stream innerStream, CancellationToken cancellationToken)
    {
        // Load custom CA certificate if specified
        if (!string.IsNullOrEmpty(_settings.TlsCaCertificatePath))
        {
            _customCaCertificate = new X509Certificate2(_settings.TlsCaCertificatePath);
        }

        _sslStream = new SslStream(
            innerStream,
            leaveInnerStreamOpen: false,
            ValidateServerCertificate);

        var options = new SslClientAuthenticationOptions
        {
            TargetHost = _settings.Host,
            EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
        };

        // Add client certificate for mTLS if specified
        if (_settings.TlsClientCertificate != null)
        {
            options.ClientCertificates = new X509CertificateCollection { _settings.TlsClientCertificate };
        }

        try
        {
            await _sslStream.AuthenticateAsClientAsync(options, cancellationToken);
        }
        catch (AuthenticationException ex)
        {
            throw new ClickHouseConnectionException($"TLS authentication failed: {ex.Message}", ex);
        }

        return _sslStream;
    }

    private bool ValidateServerCertificate(
        object sender,
        X509Certificate? certificate,
        X509Chain? chain,
        SslPolicyErrors sslPolicyErrors)
    {
        // If insecure TLS is allowed, accept any certificate
        if (_settings.AllowInsecureTls)
            return true;

        // No errors - certificate is valid
        if (sslPolicyErrors == SslPolicyErrors.None)
            return true;

        // If we have a custom CA certificate, validate against it
        if (_customCaCertificate != null && certificate != null)
        {
            // Create a new chain with our custom CA
            using var customChain = new X509Chain();
            customChain.ChainPolicy.ExtraStore.Add(_customCaCertificate);
            customChain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
            customChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;

            var cert2 = certificate as X509Certificate2 ?? new X509Certificate2(certificate);
            if (customChain.Build(cert2))
            {
                // Verify the chain ends with our custom CA
                var rootCert = customChain.ChainElements[^1].Certificate;
                if (rootCert.Thumbprint == _customCaCertificate.Thumbprint)
                    return true;
            }
        }

        // Certificate validation failed
        return false;
    }

    private async Task PerformHandshakeAsync(CancellationToken cancellationToken)
    {
        await SendClientHelloAsync(cancellationToken);
        ServerInfo = await ReceiveServerHelloAsync(cancellationToken);
        NegotiatedProtocolVersion = Math.Min(ProtocolVersion.Current, ServerInfo.ProtocolRevision);

        // For protocol versions >= WithAddendum, send client hello addendum to server
        if (NegotiatedProtocolVersion >= ProtocolVersion.WithAddendum)
        {
            await SendHelloAddendumAsync(cancellationToken);
        }
    }

    private async Task SendClientHelloAsync(CancellationToken cancellationToken)
    {
        var clientHello = ClientHello.Create(
            _settings.ClientName,
            _settings.Database,
            _settings.Username,
            _settings.Password);

        var bufferWriter = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(bufferWriter);
        clientHello.Write(ref writer);

        await _pipeWriter!.WriteAsync(bufferWriter.WrittenMemory, cancellationToken);
        await _pipeWriter.FlushAsync(cancellationToken);
    }

    private async Task<ServerHello> ReceiveServerHelloAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            var result = await _pipeReader!.ReadAsync(cancellationToken);
            var buffer = result.Buffer;

            if (result.IsCanceled)
                throw new OperationCanceledException(cancellationToken);

            if (buffer.IsEmpty && result.IsCompleted)
                throw new ClickHouseConnectionException("Server closed connection during handshake");

            try
            {
                var reader = new ProtocolReader(buffer);
                var serverHello = ServerHello.Read(ref reader);
                _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                return serverHello;
            }
            catch (InvalidOperationException)
            {
                // Not enough data yet, need more
                _pipeReader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                    throw new ClickHouseConnectionException("Incomplete server hello response");
            }
        }
    }

    private async Task SendHelloAddendumAsync(CancellationToken cancellationToken)
    {
        // Send client hello addendum containing quota key (empty for regular clients)
        var bufferWriter = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(bufferWriter);
        writer.WriteString(string.Empty); // quota_key

        await _pipeWriter!.WriteAsync(bufferWriter.WrittenMemory, cancellationToken);
        await _pipeWriter.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// Closes the connection gracefully.
    /// </summary>
    public async Task CloseAsync()
    {
        if (!_isOpen)
            return;

        ClickHouseMeter.DecrementConnections();
        _logger.ConnectionClosed(_settings.Host);
        await CloseInternalAsync();
    }

    /// <summary>
    /// Cancels the currently executing query on the server.
    /// If no query is executing, this method does nothing.
    /// </summary>
    /// <remarks>
    /// This sends a Cancel message to the server which will abort the query.
    /// The query method will throw an OperationCanceledException after cancellation.
    /// The connection remains usable for subsequent queries after cancellation.
    /// </remarks>
    public async Task CancelCurrentQueryAsync()
    {
        string? queryId;
        lock (_queryLock)
        {
            queryId = _currentQueryId;
        }

        if (queryId == null)
            return; // No query to cancel

        await SendCancelAsync();
    }

    /// <summary>
    /// Gets the ID of the currently executing query, or null if no query is running.
    /// </summary>
    /// <remarks>
    /// Query IDs are auto-generated GUIDs assigned when a query starts.
    /// This can be used with <see cref="KillQueryAsync"/> to cancel queries from another connection.
    /// </remarks>
    public string? CurrentQueryId
    {
        get
        {
            lock (_queryLock)
            {
                return _currentQueryId;
            }
        }
    }

    /// <summary>
    /// Kills a query by its ID using a separate connection.
    /// </summary>
    /// <remarks>
    /// This creates a new connection to execute KILL QUERY, which is more reliable
    /// than sending a Cancel message when the original connection may be blocked.
    /// Use <see cref="CurrentQueryId"/> to get the ID of a running query.
    /// </remarks>
    /// <param name="queryId">The query ID to kill.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task KillQueryAsync(string queryId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(queryId))
            throw new ArgumentNullException(nameof(queryId));

        await using var killConnection = new ClickHouseConnection(_settings);
        await killConnection.OpenAsync(cancellationToken);

        // Use parameterized approach to avoid SQL injection
        // KILL QUERY doesn't support parameters, so we validate the queryId format
        // Query IDs are GUIDs in format xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        if (!Guid.TryParse(queryId, out _))
            throw new ArgumentException("Invalid query ID format. Expected a GUID.", nameof(queryId));

        await killConnection.ExecuteNonQueryAsync(
            $"KILL QUERY WHERE query_id = '{queryId}'",
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Creates a new command associated with this connection.
    /// </summary>
    /// <returns>A new command instance.</returns>
    public ClickHouseCommand CreateCommand()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new ClickHouseCommand(this);
    }

    /// <summary>
    /// Creates a new command with the specified SQL text.
    /// </summary>
    /// <param name="sql">The SQL command text.</param>
    /// <returns>A new command instance with the specified SQL.</returns>
    public ClickHouseCommand CreateCommand(string sql)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new ClickHouseCommand(this, sql);
    }

    /// <summary>
    /// Creates a new bulk inserter for high-performance batch inserts.
    /// </summary>
    /// <typeparam name="T">The POCO type representing a row.</typeparam>
    /// <param name="tableName">The table name to insert into.</param>
    /// <param name="options">Optional bulk insert options.</param>
    /// <returns>A new bulk inserter instance. Call InitAsync() before use.</returns>
    public BulkInserter<T> CreateBulkInserter<T>(string tableName, BulkInsertOptions? options = null)
        where T : class
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            throw new InvalidOperationException("Connection is not open.");

        return new BulkInserter<T>(this, tableName, options);
    }

    /// <summary>
    /// Bulk inserts rows from an enumerable into the specified table.
    /// </summary>
    /// <typeparam name="T">The POCO type representing a row.</typeparam>
    /// <param name="tableName">The table name to insert into.</param>
    /// <param name="rows">The rows to insert.</param>
    /// <param name="options">Optional bulk insert options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task BulkInsertAsync<T>(
        string tableName,
        IEnumerable<T> rows,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        await using var inserter = CreateBulkInserter<T>(tableName, options);
        await inserter.InitAsync(cancellationToken);

        // Use streaming path when preferred (default) for reduced GC pressure
        if (options?.PreferDirectStreaming ?? true)
            await inserter.AddRangeStreamingAsync(rows, cancellationToken);
        else
            await inserter.AddRangeAsync(rows, cancellationToken);

        await inserter.CompleteAsync(cancellationToken);
    }

    /// <summary>
    /// Bulk inserts rows from an async enumerable into the specified table.
    /// Enables streaming inserts with bounded memory usage.
    /// </summary>
    /// <typeparam name="T">The POCO type representing a row.</typeparam>
    /// <param name="tableName">The table name to insert into.</param>
    /// <param name="rows">The async enumerable of rows to insert.</param>
    /// <param name="options">Optional bulk insert options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task BulkInsertAsync<T>(
        string tableName,
        IAsyncEnumerable<T> rows,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        await using var inserter = CreateBulkInserter<T>(tableName, options);
        await inserter.InitAsync(cancellationToken);

        // Use streaming path when preferred (default) for reduced GC pressure
        if (options?.PreferDirectStreaming ?? true)
        {
            await inserter.AddRangeStreamingAsync(rows, cancellationToken);
        }
        else
        {
            await foreach (var row in rows.WithCancellation(cancellationToken))
            {
                await inserter.AddAsync(row, cancellationToken);
            }
        }

        await inserter.CompleteAsync(cancellationToken);
    }

    /// <summary>
    /// Executes a query and returns the first column of the first row.
    /// </summary>
    /// <typeparam name="T">The expected result type.</typeparam>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="progress">Optional progress reporter.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The scalar result, or default if no rows returned.</returns>
    public async Task<T?> ExecuteScalarAsync<T>(
        string sql,
        IProgress<QueryProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            throw new InvalidOperationException("Connection is not open.");

        var queryId = Guid.NewGuid().ToString();
        using var activity = ClickHouseActivitySource.StartQuery(sql, queryId, _settings.Database, _settings.Telemetry);
        var stopwatch = Stopwatch.StartNew();
        long rowsRead = 0;
        var success = false;

        _logger.LogQueryStarted(queryId, sql);

        try
        {
            await SendQueryAsync(sql, cancellationToken);

            // Register cancellation callback to send Cancel message to server
            await using var registration = cancellationToken.Register(() => _ = SendCancelAsync());

            object? result = null;
            bool hasResult = false;

            await foreach (var message in ReadServerMessagesAsync(cancellationToken))
            {
                switch (message)
                {
                    case ProgressMessage progressMessage:
                        progress?.Report(progressMessage.ToQueryProgress());
                        rowsRead = (long)progressMessage.Rows;
                        break;

                    case DataMessage dataMessage:
                        if (!hasResult && dataMessage.Block.RowCount > 0 && dataMessage.Block.ColumnCount > 0)
                        {
                            result = dataMessage.Block.GetValue(0, 0);
                            hasResult = true;
                        }
                        break;

                    case EndOfStreamMessage:
                        success = true;
                        ClickHouseActivitySource.SetQueryResults(activity, hasResult ? 1 : 0, 0);
                        return ConvertResult<T>(result);
                }
            }

            return default;
        }
        catch (OperationCanceledException) when (_cancellationRequested)
        {
            // Server cancellation was requested - drain remaining messages to reset connection state
            await DrainAfterCancellationAsync();
            throw;
        }
        catch (Exception ex)
        {
            ClickHouseActivitySource.SetError(activity, ex);
            ClickHouseMeter.ErrorsTotal.Add(1);
            _logger.QueryFailed(queryId, ex.Message);
            throw;
        }
        finally
        {
            stopwatch.Stop();
            ClickHouseMeter.RecordQuery(_settings.Database, stopwatch.Elapsed, rowsRead, success);
            if (success)
                _logger.QueryCompleted(queryId, rowsRead, stopwatch.Elapsed.TotalMilliseconds);
        }
    }

    /// <summary>
    /// Executes a query that does not return rows.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="progress">Optional progress reporter.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of rows affected.</returns>
    public async Task<long> ExecuteNonQueryAsync(
        string sql,
        IProgress<QueryProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            throw new InvalidOperationException("Connection is not open.");

        var queryId = Guid.NewGuid().ToString();
        using var activity = ClickHouseActivitySource.StartQuery(sql, queryId, _settings.Database, _settings.Telemetry);
        var stopwatch = Stopwatch.StartNew();
        long totalRows = 0;
        var success = false;

        _logger.LogQueryStarted(queryId, sql);

        try
        {
            await SendQueryAsync(sql, cancellationToken);

            // Register cancellation callback to send Cancel message to server
            await using var registration = cancellationToken.Register(() => _ = SendCancelAsync());

            await foreach (var message in ReadServerMessagesAsync(cancellationToken))
            {
                switch (message)
                {
                    case ProgressMessage progressMessage:
                        progress?.Report(progressMessage.ToQueryProgress());
                        totalRows = (long)progressMessage.Rows;
                        break;

                    case DataMessage:
                        // Ignore data for non-query
                        break;

                    case EndOfStreamMessage:
                        success = true;
                        ClickHouseActivitySource.SetQueryResults(activity, totalRows, 0);
                        return totalRows;
                }
            }

            return totalRows;
        }
        catch (OperationCanceledException) when (_cancellationRequested)
        {
            // Server cancellation was requested - drain remaining messages to reset connection state
            await DrainAfterCancellationAsync();
            throw;
        }
        catch (Exception ex)
        {
            ClickHouseActivitySource.SetError(activity, ex);
            ClickHouseMeter.ErrorsTotal.Add(1);
            _logger.QueryFailed(queryId, ex.Message);
            throw;
        }
        finally
        {
            stopwatch.Stop();
            ClickHouseMeter.RecordQuery(_settings.Database, stopwatch.Elapsed, totalRows, success);
            if (success)
                _logger.QueryCompleted(queryId, totalRows, stopwatch.Elapsed.TotalMilliseconds);
        }
    }

    /// <summary>
    /// Executes a query and returns a data reader for streaming results.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A data reader for iterating through results.</returns>
    public async Task<ClickHouseDataReader> ExecuteReaderAsync(
        string sql,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            throw new InvalidOperationException("Connection is not open.");

        var queryId = Guid.NewGuid().ToString();
        // Note: Activity is started here but will continue throughout the reader's lifetime.
        // For full duration tracking, the ClickHouseDataReader would need to record when disposed.
        var activity = ClickHouseActivitySource.StartQuery(sql, queryId, _settings.Database, _settings.Telemetry);
        _logger.LogQueryStarted(queryId, sql);

        try
        {
            await SendQueryAsync(sql, cancellationToken);

            var enumerator = ReadServerMessagesAsync(cancellationToken)
                .GetAsyncEnumerator(cancellationToken);

            return new ClickHouseDataReader(enumerator, this, activity);
        }
        catch (Exception ex)
        {
            activity?.Dispose();
            ClickHouseActivitySource.SetError(activity, ex);
            ClickHouseMeter.ErrorsTotal.Add(1);
            _logger.QueryFailed(queryId, ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Executes a query and returns an async enumerable of rows.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of rows.</returns>
    public async IAsyncEnumerable<ClickHouseRow> QueryAsync(
        string sql,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var reader = await ExecuteReaderAsync(sql, cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            yield return new ClickHouseRow(reader);
        }
    }

    /// <summary>
    /// Executes a query and returns an async enumerable of mapped objects.
    /// </summary>
    /// <typeparam name="T">The type to map rows to. Must have a parameterless constructor.</typeparam>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of mapped objects.</returns>
    public async IAsyncEnumerable<T> QueryAsync<T>(
        string sql,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : new()
    {
        await using var reader = await ExecuteReaderAsync(sql, cancellationToken);

        // Need to call ReadAsync at least once to initialize schema before creating mapper
        if (!await reader.ReadAsync(cancellationToken))
            yield break;

        var mapper = new TypeMapper<T>(reader);

        // Map the first row
        yield return mapper.Map(reader);

        // Map remaining rows
        while (await reader.ReadAsync(cancellationToken))
        {
            yield return mapper.Map(reader);
        }
    }

    /// <summary>
    /// Executes a query and returns an async enumerable of mapped objects using the fast typed path.
    /// This method avoids boxing for primitive types, providing significantly better performance
    /// for large result sets.
    /// </summary>
    /// <typeparam name="T">The type to map rows to. Must have a parameterless constructor.</typeparam>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of mapped objects.</returns>
    public async IAsyncEnumerable<T> QueryTypedAsync<T>(
        string sql,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : new()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            throw new InvalidOperationException("Connection is not open.");

        var queryId = Guid.NewGuid().ToString();
        using var activity = ClickHouseActivitySource.StartQuery(sql, queryId, _settings.Database, _settings.Telemetry);
        var stopwatch = Stopwatch.StartNew();
        long totalRows = 0;
        var success = false;

        _logger.LogQueryStarted(queryId, sql);

        Mapping.ITypedRowMapper<T>? mapper = null;

        try
        {
            await SendQueryAsync(sql, cancellationToken);

            // Register cancellation callback to send Cancel message to server
            await using var registration = cancellationToken.Register(() => _ = SendCancelAsync());

            await foreach (var block in ReadTypedBlocksAsync(cancellationToken))
            {
                using (block)
                {
                    if (block.RowCount == 0)
                        continue;

                    // Create mapper on first non-empty block
                    mapper ??= Mapping.TypedRowMapperFactory.GetMapper<T>(block.ColumnNames);

                    // Map all rows in this block
                    for (int i = 0; i < block.RowCount; i++)
                    {
                        totalRows++;
                        yield return mapper.MapRow(block.Columns, i);
                    }
                }
            }

            success = true;
            ClickHouseActivitySource.SetQueryResults(activity, totalRows, 0);
        }
        finally
        {
            stopwatch.Stop();
            ClickHouseMeter.RecordQuery(_settings.Database, stopwatch.Elapsed, totalRows, success);
            if (success)
                _logger.QueryCompleted(queryId, totalRows, stopwatch.Elapsed.TotalMilliseconds);
            else
                ClickHouseMeter.ErrorsTotal.Add(1);
        }
    }

    /// <summary>
    /// Reads typed blocks from the server message stream.
    /// </summary>
    private async IAsyncEnumerable<TypedBlock> ReadTypedBlocksAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var registry = ColumnReaderRegistry.Default;

        try
        {
            while (true)
            {
                var result = await _pipeReader!.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (result.IsCanceled)
                    throw new OperationCanceledException(cancellationToken);

                if (buffer.IsEmpty && result.IsCompleted)
                    throw new ClickHouseConnectionException("Server closed connection unexpectedly");

                bool messageRead = false;
                while (TryReadTypedMessage(ref buffer, registry, out var message, out var typedBlock))
                {
                    messageRead = true;

                    if (message is EndOfStreamMessage)
                    {
                        _pipeReader.AdvanceTo(buffer.Start);
                        yield break;
                    }

                    if (typedBlock != null)
                    {
                        _pipeReader.AdvanceTo(buffer.Start);
                        yield return typedBlock;
                        // Need to re-read buffer after yielding
                        break;
                    }
                }

                // Advance to consumed position, examined to end
                _pipeReader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted && !messageRead)
                    throw new ClickHouseConnectionException("Incomplete response from server");
            }
        }
        finally
        {
            // Clear query tracking when done (success or error)
            lock (_queryLock)
            {
                _currentQueryId = null;
            }
        }
    }

    private bool TryReadTypedMessage(ref ReadOnlySequence<byte> buffer, ColumnReaderRegistry registry, out object? message, out TypedBlock? typedBlock)
    {
        message = null;
        typedBlock = null;

        if (buffer.IsEmpty)
            return false;

        try
        {
            var reader = new ProtocolReader(buffer);
            var messageType = (ServerMessageType)reader.ReadVarInt();

            switch (messageType)
            {
                case ServerMessageType.Data:
                    // For uncompressed data, do a non-allocating scan pass first
                    // to validate block completeness before parsing
                    if (!_compressionEnabled)
                    {
                        // Create a scanner positioned after the message type
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanUncompressedDataMessage(scanBuffer, out var messageLength))
                        {
                            // Not enough data yet - don't consume buffer, wait for more
                            return false;
                        }

                        // Buffer into contiguous memory if fragmented for faster parsing
                        // This matches what the compressed path does after decompression
                        if (!scanBuffer.IsSingleSegment)
                        {
                            var pool = ArrayPool<byte>.Shared;
                            var contiguous = pool.Rent((int)messageLength);
                            try
                            {
                                scanBuffer.Slice(0, messageLength).CopyTo(contiguous);
                                var contiguousSeq = new ReadOnlySequence<byte>(contiguous, 0, (int)messageLength);
                                var contiguousReader = new ProtocolReader(contiguousSeq);

                                // Read table name and block from contiguous buffer
                                var tableName = contiguousReader.ReadString();
                                typedBlock = Block.ReadTypedBlockWithTableName(ref contiguousReader, registry, tableName, NegotiatedProtocolVersion);

                                // Advance original buffer past message type + message content
                                buffer = buffer.Slice(reader.Consumed + messageLength);
                                return true;
                            }
                            finally
                            {
                                pool.Return(contiguous);
                            }
                        }
                    }

                    typedBlock = ReadTypedDataMessage(ref reader, registry);
                    buffer = buffer.Slice(reader.Consumed);
                    return true;

                case ServerMessageType.Exception:
                    var exceptionMessage = ExceptionMessage.Read(ref reader);
                    buffer = buffer.Slice(reader.Consumed);
                    throw ClickHouseServerException.FromExceptionMessage(exceptionMessage);

                case ServerMessageType.Progress:
                    var progressMessage = ProgressMessage.Read(ref reader, NegotiatedProtocolVersion);
                    buffer = buffer.Slice(reader.Consumed);
                    message = progressMessage;
                    return true;

                case ServerMessageType.EndOfStream:
                    buffer = buffer.Slice(reader.Consumed);
                    message = EndOfStreamMessage.Instance;
                    return true;

                case ServerMessageType.ProfileInfo:
                    SkipProfileInfo(ref reader);
                    buffer = buffer.Slice(reader.Consumed);
                    return TryReadTypedMessage(ref buffer, registry, out message, out typedBlock);

                case ServerMessageType.ProfileEvents:
                    // ProfileEvents - read and discard using regular block (not typed)
                    // For uncompressed data, do a scan pass first
                    if (!_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanUncompressedDataMessage(scanBuffer, out _))
                            return false;
                    }
                    ReadDataMessage(ref reader, registry);
                    buffer = buffer.Slice(reader.Consumed);
                    return TryReadTypedMessage(ref buffer, registry, out message, out typedBlock);

                case ServerMessageType.Totals:
                case ServerMessageType.Extremes:
                    // These are data blocks - read as typed
                    // For uncompressed data, do a scan pass first
                    if (!_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanUncompressedDataMessage(scanBuffer, out var totalsMessageLength))
                            return false;

                        // Buffer into contiguous memory if fragmented for faster parsing
                        if (!scanBuffer.IsSingleSegment)
                        {
                            var pool = ArrayPool<byte>.Shared;
                            var contiguous = pool.Rent((int)totalsMessageLength);
                            try
                            {
                                scanBuffer.Slice(0, totalsMessageLength).CopyTo(contiguous);
                                var contiguousSeq = new ReadOnlySequence<byte>(contiguous, 0, (int)totalsMessageLength);
                                var contiguousReader = new ProtocolReader(contiguousSeq);

                                var tableName = contiguousReader.ReadString();
                                typedBlock = Block.ReadTypedBlockWithTableName(ref contiguousReader, registry, tableName, NegotiatedProtocolVersion);

                                buffer = buffer.Slice(reader.Consumed + totalsMessageLength);
                                return true;
                            }
                            finally
                            {
                                pool.Return(contiguous);
                            }
                        }
                    }
                    typedBlock = ReadTypedDataMessage(ref reader, registry);
                    buffer = buffer.Slice(reader.Consumed);
                    return true;

                default:
                    throw new ClickHouseException($"Unexpected server message type: {messageType}");
            }
        }
        catch (InvalidOperationException)
        {
            // Not enough data yet
            return false;
        }
    }

    /// <summary>
    /// Scans an uncompressed data message without allocating arrays.
    /// This is the first pass of two-pass parsing for uncompressed data.
    /// </summary>
    /// <param name="buffer">The buffer positioned after the message type.</param>
    /// <param name="messageLength">The total length of the message in bytes.</param>
    /// <returns>True if the entire message is available; false if not enough data.</returns>
    private bool TryScanUncompressedDataMessage(ReadOnlySequence<byte> buffer, out long messageLength)
    {
        messageLength = 0;
        var scanner = new ProtocolReader(buffer);

        // Skip table name
        if (!scanner.TrySkipString())
            return false;

        // Read block header (BlockInfo, columnCount, rowCount)
        var header = Block.TryReadBlockHeader(ref scanner);
        if (header == null)
            return false;

        // Skip all column data
        var skipperRegistry = ColumnSkipperRegistry.Default;
        if (!Block.TrySkipBlockColumns(ref scanner, skipperRegistry,
            header.Value.ColumnCount, header.Value.RowCount, NegotiatedProtocolVersion))
        {
            return false;
        }

        messageLength = scanner.Consumed;
        return true;
    }

    private TypedBlock ReadTypedDataMessage(ref ProtocolReader reader, ColumnReaderRegistry registry)
    {
        if (!_compressionEnabled)
        {
            // Scan pass already validated completeness in TryReadTypedMessage
            // Just parse directly now - we know we have all the data
            var tableName = reader.ReadString();
            return Block.ReadTypedBlockWithTableName(ref reader, registry, tableName, NegotiatedProtocolVersion);
        }

        // Table name is read OUTSIDE the compressed block
        var tableNameFromCompressed = reader.ReadString();

        bool isCompressed = IsNextBlockCompressed(ref reader);

        if (isCompressed)
        {
            // Read and decompress the block data
            var compressedData = CompressedBlock.ReadFromProtocol(ref reader);
            var decompressed = CompressedBlock.Decompress(compressedData.Span);

            var decompressedSequence = new ReadOnlySequence<byte>(decompressed);
            var blockReader = new ProtocolReader(decompressedSequence);

            return Block.ReadTypedBlockWithTableName(ref blockReader, registry, tableNameFromCompressed, NegotiatedProtocolVersion);
        }
        else
        {
            // Data is not compressed - read directly
            return Block.ReadTypedBlockWithTableName(ref reader, registry, tableNameFromCompressed, NegotiatedProtocolVersion);
        }
    }

    /// <summary>
    /// Internal method for executing a parameterized scalar query.
    /// Used by ClickHouseCommand.
    /// </summary>
    internal async Task<T?> ExecuteScalarWithParametersAsync<T>(
        string sql,
        ClickHouseParameterCollection parameters,
        IProgress<QueryProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            throw new InvalidOperationException("Connection is not open.");

        var (rewrittenSql, settings) = SqlParameterRewriter.Process(sql, parameters);
        await SendQueryAsync(rewrittenSql, settings, cancellationToken);

        // Register cancellation callback to send Cancel message to server
        await using var registration = cancellationToken.Register(() => _ = SendCancelAsync());

        object? result = null;
        bool hasResult = false;

        try
        {
            await foreach (var message in ReadServerMessagesAsync(cancellationToken))
            {
                switch (message)
                {
                    case ProgressMessage progressMessage:
                        progress?.Report(progressMessage.ToQueryProgress());
                        break;

                    case DataMessage dataMessage:
                        if (!hasResult && dataMessage.Block.RowCount > 0 && dataMessage.Block.ColumnCount > 0)
                        {
                            result = dataMessage.Block.GetValue(0, 0);
                            hasResult = true;
                        }
                        break;

                    case EndOfStreamMessage:
                        return ConvertResult<T>(result);
                }
            }
        }
        catch (OperationCanceledException) when (_cancellationRequested)
        {
            await DrainAfterCancellationAsync();
            throw;
        }

        return default;
    }

    /// <summary>
    /// Internal method for executing a parameterized non-query.
    /// Used by ClickHouseCommand.
    /// </summary>
    internal async Task<long> ExecuteNonQueryWithParametersAsync(
        string sql,
        ClickHouseParameterCollection parameters,
        IProgress<QueryProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            throw new InvalidOperationException("Connection is not open.");

        var (rewrittenSql, settings) = SqlParameterRewriter.Process(sql, parameters);
        await SendQueryAsync(rewrittenSql, settings, cancellationToken);

        // Register cancellation callback to send Cancel message to server
        await using var registration = cancellationToken.Register(() => _ = SendCancelAsync());

        long totalRows = 0;

        try
        {
            await foreach (var message in ReadServerMessagesAsync(cancellationToken))
            {
                switch (message)
                {
                    case ProgressMessage progressMessage:
                        progress?.Report(progressMessage.ToQueryProgress());
                        totalRows = (long)progressMessage.Rows;
                        break;

                    case DataMessage:
                        // Ignore data for non-query
                        break;

                    case EndOfStreamMessage:
                        return totalRows;
                }
            }
        }
        catch (OperationCanceledException) when (_cancellationRequested)
        {
            await DrainAfterCancellationAsync();
            throw;
        }

        return totalRows;
    }

    /// <summary>
    /// Internal method for executing a parameterized reader query.
    /// Used by ClickHouseCommand.
    /// </summary>
    internal async Task<ClickHouseDataReader> ExecuteReaderWithParametersAsync(
        string sql,
        ClickHouseParameterCollection parameters,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            throw new InvalidOperationException("Connection is not open.");

        var (rewrittenSql, settings) = SqlParameterRewriter.Process(sql, parameters);
        await SendQueryAsync(rewrittenSql, settings, cancellationToken);

        var enumerator = ReadServerMessagesAsync(cancellationToken)
            .GetAsyncEnumerator(cancellationToken);

        return new ClickHouseDataReader(enumerator, this);
    }

    private Task SendQueryAsync(string sql, CancellationToken cancellationToken)
        => SendQueryAsync(sql, null, cancellationToken);

    private async Task SendQueryAsync(
        string sql,
        IReadOnlyDictionary<string, string>? parameters,
        CancellationToken cancellationToken)
    {
        // Set compression state for response handling
        _compressionEnabled = _settings.Compress;
        _cancellationRequested = false;

        var queryMessage = QueryMessage.Create(
            sql,
            _settings.ClientName,
            _settings.Username,
            NegotiatedProtocolVersion,
            useCompression: _settings.Compress,
            parameters: parameters);

        // Track the query ID for cancellation support
        lock (_queryLock)
        {
            _currentQueryId = queryMessage.QueryId;
        }

        var bufferWriter = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(bufferWriter);

        // Write query message
        queryMessage.Write(ref writer, NegotiatedProtocolVersion);

        // Write empty data block (required after query)
        // When compression is enabled, the client must also send compressed data blocks
        if (_settings.Compress)
        {
            var compressor = CompressedBlock.GetCompressor(_settings.CompressionMethod);
            Block.WriteEmpty(ref writer, compressor);
        }
        else
        {
            Block.WriteEmpty(ref writer);
        }

        await _pipeWriter!.WriteAsync(bufferWriter.WrittenMemory, cancellationToken);
        await _pipeWriter.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// Sends a cancel message to the server to abort the current query.
    /// This is an internal method called when cancellation is requested.
    /// </summary>
    internal async Task SendCancelAsync()
    {
        if (_pipeWriter == null || !_isOpen)
            return;

        try
        {
            var bufferWriter = new ArrayBufferWriter<byte>();
            var writer = new ProtocolWriter(bufferWriter);
            CancelMessage.Write(ref writer);

            await _pipeWriter.WriteAsync(bufferWriter.WrittenMemory);
            await _pipeWriter.FlushAsync();

            _cancellationRequested = true;
        }
        catch
        {
            // Best effort - connection may already be closed
        }
    }

    /// <summary>
    /// Drains remaining server messages after a cancellation to reset connection state.
    /// After Cancel is sent, the server responds with either Exception or EndOfStream.
    /// </summary>
    private async Task DrainAfterCancellationAsync()
    {
        if (_pipeReader == null || !_isOpen)
            return;

        var registry = ColumnReaderRegistry.Default;

        try
        {
            // Use a short timeout for draining - we don't want to wait forever
            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            while (true)
            {
                var result = await _pipeReader.ReadAsync(timeoutCts.Token);
                var buffer = result.Buffer;

                if (buffer.IsEmpty && result.IsCompleted)
                    break;

                while (TryReadMessage(ref buffer, registry, out var message))
                {
                    if (message is EndOfStreamMessage)
                    {
                        _pipeReader.AdvanceTo(buffer.Start);
                        return;
                    }
                }

                _pipeReader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                    break;
            }
        }
        catch (ClickHouseServerException)
        {
            // Expected - server sends exception for cancelled query
            // Connection is now in clean state
        }
        catch
        {
            // Timeout or other error - connection may be in bad state
            // but we've done our best to drain
        }
        finally
        {
            lock (_queryLock)
            {
                _currentQueryId = null;
            }
        }
    }

    private async IAsyncEnumerable<object> ReadServerMessagesAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var registry = ColumnReaderRegistry.Default;

        try
        {
            while (true)
            {
                var result = await _pipeReader!.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (result.IsCanceled)
                    throw new OperationCanceledException(cancellationToken);

                if (buffer.IsEmpty && result.IsCompleted)
                    throw new ClickHouseConnectionException("Server closed connection unexpectedly");

                bool messageRead = false;
                while (TryReadMessage(ref buffer, registry, out var message))
                {
                    messageRead = true;

                    if (message is EndOfStreamMessage)
                    {
                        _pipeReader.AdvanceTo(buffer.Start);
                        yield return message;
                        yield break;
                    }

                    yield return message;
                }

                // Advance to consumed position, examined to end
                _pipeReader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted && !messageRead)
                    throw new ClickHouseConnectionException("Incomplete response from server");
            }
        }
        finally
        {
            // Clear query tracking when done (success or error)
            lock (_queryLock)
            {
                _currentQueryId = null;
            }
        }
    }

    private bool TryReadMessage(ref ReadOnlySequence<byte> buffer, ColumnReaderRegistry registry, out object message)
    {
        message = null!;

        if (buffer.IsEmpty)
            return false;

        try
        {
            var reader = new ProtocolReader(buffer);
            var messageType = (ServerMessageType)reader.ReadVarInt();

            switch (messageType)
            {
                case ServerMessageType.Data:
                    // For uncompressed data, do a non-allocating scan pass first
                    // to validate block completeness before parsing
                    if (!_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanUncompressedDataMessage(scanBuffer, out var dataMessageLength))
                        {
                            return false;
                        }

                        // Buffer into contiguous memory if fragmented for faster parsing
                        if (!scanBuffer.IsSingleSegment)
                        {
                            var pool = ArrayPool<byte>.Shared;
                            var contiguous = pool.Rent((int)dataMessageLength);
                            try
                            {
                                scanBuffer.Slice(0, dataMessageLength).CopyTo(contiguous);
                                var contiguousSeq = new ReadOnlySequence<byte>(contiguous, 0, (int)dataMessageLength);
                                var contiguousReader = new ProtocolReader(contiguousSeq);

                                var tableName = contiguousReader.ReadString();
                                var block = Block.ReadTypedBlockWithTableName(ref contiguousReader, registry, tableName, NegotiatedProtocolVersion);

                                buffer = buffer.Slice(reader.Consumed + dataMessageLength);
                                message = new DataMessage { Block = block };
                                return true;
                            }
                            finally
                            {
                                pool.Return(contiguous);
                            }
                        }
                    }

                    var dataMessage = ReadDataMessage(ref reader, registry);
                    buffer = buffer.Slice(reader.Consumed);
                    message = dataMessage;
                    return true;

                case ServerMessageType.Exception:
                    var exceptionMessage = ExceptionMessage.Read(ref reader);
                    buffer = buffer.Slice(reader.Consumed);
                    throw ClickHouseServerException.FromExceptionMessage(exceptionMessage);

                case ServerMessageType.Progress:
                    var progressMessage = ProgressMessage.Read(ref reader, NegotiatedProtocolVersion);
                    buffer = buffer.Slice(reader.Consumed);
                    message = progressMessage;
                    return true;

                case ServerMessageType.EndOfStream:
                    buffer = buffer.Slice(reader.Consumed);
                    message = EndOfStreamMessage.Instance;
                    return true;

                case ServerMessageType.ProfileInfo:
                    // Skip profile info for now - read and discard
                    SkipProfileInfo(ref reader);
                    buffer = buffer.Slice(reader.Consumed);
                    // Continue reading next message
                    return TryReadMessage(ref buffer, registry, out message);

                case ServerMessageType.ProfileEvents:
                    // ProfileEvents is a data block containing profiling information
                    // Read and discard it
                    // For uncompressed data, do a scan pass first
                    if (!_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanUncompressedDataMessage(scanBuffer, out _))
                            return false;
                    }
                    var profileEventsBlock = ReadDataMessage(ref reader, registry);
                    buffer = buffer.Slice(reader.Consumed);
                    // Continue reading next message
                    return TryReadMessage(ref buffer, registry, out message);

                case ServerMessageType.Totals:
                case ServerMessageType.Extremes:
                    // These are data blocks, read them
                    // For uncompressed data, do a scan pass first
                    if (!_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanUncompressedDataMessage(scanBuffer, out var specialMessageLength))
                            return false;

                        // Buffer into contiguous memory if fragmented for faster parsing
                        if (!scanBuffer.IsSingleSegment)
                        {
                            var pool = ArrayPool<byte>.Shared;
                            var contiguous = pool.Rent((int)specialMessageLength);
                            try
                            {
                                scanBuffer.Slice(0, specialMessageLength).CopyTo(contiguous);
                                var contiguousSeq = new ReadOnlySequence<byte>(contiguous, 0, (int)specialMessageLength);
                                var contiguousReader = new ProtocolReader(contiguousSeq);

                                var tableName = contiguousReader.ReadString();
                                var block = Block.ReadTypedBlockWithTableName(ref contiguousReader, registry, tableName, NegotiatedProtocolVersion);

                                buffer = buffer.Slice(reader.Consumed + specialMessageLength);
                                message = new DataMessage { Block = block };
                                return true;
                            }
                            finally
                            {
                                pool.Return(contiguous);
                            }
                        }
                    }
                    var specialData = ReadDataMessage(ref reader, registry);
                    buffer = buffer.Slice(reader.Consumed);
                    message = specialData;
                    return true;

                default:
                    throw new ClickHouseException($"Unexpected server message type: {messageType}");
            }
        }
        catch (InvalidOperationException)
        {
            // Not enough data yet
            return false;
        }
    }

    private DataMessage ReadDataMessage(ref ProtocolReader reader, ColumnReaderRegistry registry)
    {
        if (!_compressionEnabled)
        {
            // Scan pass already validated completeness in TryReadMessage
            // Just parse directly now - we know we have all the data
            var tableName = reader.ReadString();
            var block = Block.ReadTypedBlockWithTableName(ref reader, registry, tableName, NegotiatedProtocolVersion);
            return new DataMessage { Block = block };
        }

        // Table name is read OUTSIDE the compressed block (per Python clickhouse-driver)
        var compressedTableName = reader.ReadString();

        // Check if the data is actually compressed by looking at the algorithm ID
        // The algorithm ID would be at offset 16 (after the 16-byte checksum)
        // Valid values: 0x82 (LZ4), 0x90 (Zstd)
        // Some messages (like ProfileEvents) may be sent uncompressed even when compression is enabled
        bool isCompressed = IsNextBlockCompressed(ref reader);

        if (isCompressed)
        {
            // Read and decompress the block data (BlockInfo + columns)
            var compressedData = CompressedBlock.ReadFromProtocol(ref reader);
            var decompressed = CompressedBlock.Decompress(compressedData.Span);

            // Parse the decompressed block using pre-read table name
            var decompressedSequence = new ReadOnlySequence<byte>(decompressed);
            var blockReader = new ProtocolReader(decompressedSequence);

            return new DataMessage { Block = Block.ReadTypedBlockWithTableName(ref blockReader, registry, compressedTableName, NegotiatedProtocolVersion) };
        }
        else
        {
            // Data is not compressed - read directly
            return new DataMessage { Block = Block.ReadTypedBlockWithTableName(ref reader, registry, compressedTableName, NegotiatedProtocolVersion) };
        }
    }

    /// <summary>
    /// Checks if the next block in the reader is compressed by examining the algorithm ID byte.
    /// </summary>
    private static bool IsNextBlockCompressed(ref ProtocolReader reader)
    {
        // A compressed block has:
        // - 16 bytes: checksum
        // - 1 byte: algorithm ID (0x82 = LZ4, 0x90 = Zstd)
        // We need at least 17 bytes to check
        if (reader.Remaining < 17)
            return false;

        // Peek at the algorithm ID byte at offset 16 (after checksum) without consuming bytes
        var algorithmId = reader.PeekByte(16);

        // Check if it's a valid compression algorithm
        return algorithmId == 0x82 || algorithmId == 0x90;
    }

    private static void SkipProfileInfo(ref ProtocolReader reader)
    {
        // ProfileInfo structure:
        // - rows (VarInt)
        // - blocks (VarInt)
        // - bytes (VarInt)
        // - applied_limit (UInt8)
        // - rows_before_limit (VarInt)
        // - calculated_rows_before_limit (UInt8)
        reader.ReadVarInt(); // rows
        reader.ReadVarInt(); // blocks
        reader.ReadVarInt(); // bytes
        reader.ReadByte();   // applied_limit
        reader.ReadVarInt(); // rows_before_limit
        reader.ReadByte();   // calculated_rows_before_limit
    }

    private static T? ConvertResult<T>(object? value)
    {
        if (value is null)
            return default;

        if (value is T typedValue)
            return typedValue;

        // Handle numeric conversions
        return (T)Convert.ChangeType(value, typeof(T));
    }

    private async Task CloseInternalAsync()
    {
        _isOpen = false;
        _compressionEnabled = false;

        if (_pipeWriter != null)
        {
            await _pipeWriter.CompleteAsync();
            _pipeWriter = null;
        }

        if (_pipeReader != null)
        {
            await _pipeReader.CompleteAsync();
            _pipeReader = null;
        }

        // Dispose SSL stream if used (this also closes the underlying network stream)
        if (_sslStream != null)
        {
            await _sslStream.DisposeAsync();
            _sslStream = null;
        }

        _networkStream = null;

        _tcpClient?.Dispose();
        _tcpClient = null;

        // Dispose custom CA certificate if loaded
        _customCaCertificate?.Dispose();
        _customCaCertificate = null;

        ServerInfo = null;
        NegotiatedProtocolVersion = 0;
    }

    /// <summary>
    /// Disposes the connection asynchronously.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        if (_isOpen)
        {
            ClickHouseMeter.DecrementConnections();
            _logger.ConnectionClosed(_settings.Host);
        }

        _disposed = true;
        await CloseInternalAsync();
    }

    #region Bulk Insert Support

    /// <summary>
    /// Sends an INSERT query to the server with an initial empty block.
    /// The server will respond with a schema block defining the expected columns.
    /// </summary>
    /// <param name="sql">The INSERT SQL (e.g., "INSERT INTO table (col1, col2) VALUES").</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    internal async Task SendInsertQueryAsync(string sql, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            throw new InvalidOperationException("Connection is not open.");

        // Set compression state for response handling
        _compressionEnabled = _settings.Compress;

        var queryMessage = QueryMessage.Create(
            sql,
            _settings.ClientName,
            _settings.Username,
            NegotiatedProtocolVersion,
            useCompression: _settings.Compress,
            parameters: null);

        var bufferWriter = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(bufferWriter);

        // Write query message
        queryMessage.Write(ref writer, NegotiatedProtocolVersion);

        // Write initial empty data block (required to trigger schema response)
        if (_settings.Compress)
        {
            var compressor = CompressedBlock.GetCompressor(_settings.CompressionMethod);
            Block.WriteEmpty(ref writer, compressor);
        }
        else
        {
            Block.WriteEmpty(ref writer);
        }

        // Wire dump for debugging protocol issues
        var wireDump = Environment.GetEnvironmentVariable("CH_WIRE_DUMP");
        if (wireDump == "1")
        {
            var logPath = "/tmp/ch_wire_dump.log";
            File.AppendAllText(logPath, $"[CH] INSERT QUERY + INITIAL EMPTY: {bufferWriter.WrittenCount} bytes\n");
        }

        await _pipeWriter!.WriteAsync(bufferWriter.WrittenMemory, cancellationToken);
        await _pipeWriter.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// Receives the schema block from the server after sending an INSERT query.
    /// The schema block has 0 rows but defines the column names and types.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The schema block with column definitions.</returns>
    internal async Task<TypedBlock> ReceiveSchemaBlockAsync(CancellationToken cancellationToken)
    {
        var registry = ColumnReaderRegistry.Default;

        // Wire dump for debugging
        var wireDump = Environment.GetEnvironmentVariable("CH_WIRE_DUMP");
        if (wireDump == "1")
        {
            File.AppendAllText("/tmp/ch_wire_dump.log", "[CH] ReceiveSchemaBlockAsync: waiting for schema...\n");
        }

        while (true)
        {
            var result = await _pipeReader!.ReadAsync(cancellationToken);
            var buffer = result.Buffer;

            if (wireDump == "1")
            {
                File.AppendAllText("/tmp/ch_wire_dump.log", $"[CH] ReceiveSchemaBlockAsync: received {buffer.Length} bytes, IsCompleted={result.IsCompleted}\n");
            }

            if (result.IsCanceled)
                throw new OperationCanceledException(cancellationToken);

            if (buffer.IsEmpty && result.IsCompleted)
                throw new ClickHouseConnectionException("Server closed connection while waiting for schema block");

            try
            {
                var reader = new ProtocolReader(buffer);
                var messageType = (ServerMessageType)reader.ReadVarInt();

                if (wireDump == "1")
                {
                    File.AppendAllText("/tmp/ch_wire_dump.log", $"[CH] ReceiveSchemaBlockAsync: messageType={messageType}\n");
                }

                if (messageType == ServerMessageType.Exception)
                {
                    var exceptionMessage = ExceptionMessage.Read(ref reader);
                    _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                    throw ClickHouseServerException.FromExceptionMessage(exceptionMessage);
                }

                if (messageType == ServerMessageType.Data)
                {
                    // For uncompressed data, do a scan pass first
                    if (!_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanUncompressedDataMessage(scanBuffer, out _))
                        {
                            // Not enough data yet
                            _pipeReader.AdvanceTo(buffer.Start, buffer.End);
                            continue;
                        }
                    }

                    var dataMessage = ReadDataMessage(ref reader, registry);
                    _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                    return dataMessage.Block;
                }

                if (messageType == ServerMessageType.TableColumns)
                {
                    // TableColumns message contains: external table name (string) + columns metadata (string)
                    // We skip both and continue waiting for the Data block (matching clickhouse-cpp behavior)
                    reader.ReadString(); // external table name
                    reader.ReadString(); // columns metadata
                    _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));

                    if (wireDump == "1")
                    {
                        File.AppendAllText("/tmp/ch_wire_dump.log", "[CH] ReceiveSchemaBlockAsync: skipped TableColumns, waiting for Data...\n");
                    }
                    continue;
                }

                // Skip other message types and continue waiting
                _pipeReader.AdvanceTo(buffer.Start, buffer.End);
            }
            catch (InvalidOperationException)
            {
                // Not enough data yet, need more
                _pipeReader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                    throw new ClickHouseConnectionException("Incomplete schema block response");
            }
        }
    }

    /// <summary>
    /// Sends a data block with column data for bulk insert.
    /// </summary>
    /// <param name="columnNames">Column names matching the schema.</param>
    /// <param name="columnTypes">ClickHouse type names matching the schema.</param>
    /// <param name="columnData">Column data arrays (column-major order).</param>
    /// <param name="rowCount">Number of rows in this batch.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    internal async Task SendDataBlockAsync(
        string[] columnNames,
        string[] columnTypes,
        object?[][] columnData,
        int rowCount,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            throw new InvalidOperationException("Connection is not open.");

        var writerRegistry = ColumnWriterRegistry.Default;
        var bufferWriter = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(bufferWriter);

        // Write message type
        writer.WriteVarInt((ulong)Protocol.ClientMessageType.Data);

        // Write table name at Data message level (matching clickhouse-go's sendData structure)
        writer.WriteString(string.Empty);

        if (_settings.Compress)
        {
            var compressor = CompressedBlock.GetCompressor(_settings.CompressionMethod)!;
            Block.WriteDataCompressed(
                ref writer,
                columnNames,
                columnTypes,
                columnData,
                rowCount,
                writerRegistry,
                NegotiatedProtocolVersion,
                compressor);
        }
        else
        {
            Block.WriteData(
                ref writer,
                columnNames,
                columnTypes,
                columnData,
                rowCount,
                writerRegistry,
                NegotiatedProtocolVersion);
        }

        // Wire dump for debugging protocol issues
        var wireDump = Environment.GetEnvironmentVariable("CH_WIRE_DUMP");
        if (wireDump == "1")
        {
            var logPath = "/tmp/ch_wire_dump.log";
            File.AppendAllText(logPath, $"[CH] DATA BLOCK: {bufferWriter.WrittenCount} bytes, protocol={NegotiatedProtocolVersion}\n");
            File.AppendAllText(logPath, $"[CH] HEX: {BitConverter.ToString(bufferWriter.WrittenSpan.ToArray())}\n");
        }

        await _pipeWriter!.WriteAsync(bufferWriter.WrittenMemory, cancellationToken);
        await _pipeWriter.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// Sends an empty data block to signal the end of data for INSERT.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    internal async Task SendEmptyBlockAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            throw new InvalidOperationException("Connection is not open.");

        var bufferWriter = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(bufferWriter);

        if (_settings.Compress)
        {
            var compressor = CompressedBlock.GetCompressor(_settings.CompressionMethod);
            Block.WriteEmpty(ref writer, compressor);
        }
        else
        {
            Block.WriteEmpty(ref writer);
        }

        // Wire dump for debugging protocol issues
        var wireDump = Environment.GetEnvironmentVariable("CH_WIRE_DUMP");
        if (wireDump == "1")
        {
            var logPath = "/tmp/ch_wire_dump.log";
            File.AppendAllText(logPath, $"[CH] EMPTY BLOCK: {bufferWriter.WrittenCount} bytes\n");
            File.AppendAllText(logPath, $"[CH] HEX: {BitConverter.ToString(bufferWriter.WrittenSpan.ToArray())}\n");
        }

        await _pipeWriter!.WriteAsync(bufferWriter.WrittenMemory, cancellationToken);
        await _pipeWriter.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// Sends a data block using column extractors for direct-to-buffer writing (no boxing).
    /// </summary>
    /// <typeparam name="TRow">The row type.</typeparam>
    /// <param name="extractors">Column extractors.</param>
    /// <param name="rows">Source rows.</param>
    /// <param name="rowCount">Number of rows to write.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    internal async Task SendDataBlockDirectAsync<TRow>(
        IReadOnlyList<IColumnExtractor<TRow>> extractors,
        IReadOnlyList<TRow> rows,
        int rowCount,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            throw new InvalidOperationException("Connection is not open.");

        // Estimate buffer size to avoid resize allocations during serialization
        // Heuristic: ~32 bytes per column per row + 1KB for protocol overhead
        // This is conservative but avoids most resize operations
        var estimatedSize = (rowCount * extractors.Count * 32) + 1024;
        var bufferWriter = BufferWriterPool.Shared.Rent(estimatedSize);

        try
        {
            var writer = new ProtocolWriter(bufferWriter);

            // Write message type
            writer.WriteVarInt((ulong)Protocol.ClientMessageType.Data);

            // Write table name at Data message level (matching clickhouse-go's sendData structure)
            writer.WriteString(string.Empty);

            if (_settings.Compress)
            {
                var compressor = CompressedBlock.GetCompressor(_settings.CompressionMethod)!;
                WriteDataBlockDirectCompressed(ref writer, extractors, rows, rowCount, compressor);
            }
            else
            {
                WriteDataBlockDirect(ref writer, extractors, rows, rowCount);
            }

            // OPTIMIZATION: Clear row references immediately after serialization, before await.
            // This allows GC to collect row objects during the network I/O await,
            // reducing Gen1 GC pressure by ensuring objects don't survive across await boundaries.
            if (rows is TRow[] rowArray)
            {
                Array.Clear(rowArray, 0, rowCount);
            }

            // Wire dump for debugging protocol issues
            var wireDump = Environment.GetEnvironmentVariable("CH_WIRE_DUMP");
            if (wireDump == "1")
            {
                var logPath = "/tmp/ch_wire_dump.log";
                File.AppendAllText(logPath, $"[CH] DATA BLOCK DIRECT: {bufferWriter.WrittenCount} bytes, protocol={NegotiatedProtocolVersion}\n");
                File.AppendAllText(logPath, $"[CH] HEX: {BitConverter.ToString(bufferWriter.WrittenSpan.ToArray())}\n");
            }

            await _pipeWriter!.WriteAsync(bufferWriter.WrittenMemory, cancellationToken);
            await _pipeWriter.FlushAsync(cancellationToken);
        }
        finally
        {
            BufferWriterPool.Shared.Return(bufferWriter);
        }
    }

    private void WriteDataBlockDirect<TRow>(
        ref ProtocolWriter writer,
        IReadOnlyList<IColumnExtractor<TRow>> extractors,
        IReadOnlyList<TRow> rows,
        int rowCount)
    {
        // NOTE: Table name is now written by the caller (SendDataBlockDirectAsync) at the Data message level

        // Block info
        BlockInfo.Default.Write(ref writer);

        // Column count and row count
        writer.WriteVarInt((ulong)extractors.Count);
        writer.WriteVarInt((ulong)rowCount);

        // Write each column directly from source data
        for (int i = 0; i < extractors.Count; i++)
        {
            var extractor = extractors[i];
            writer.WriteString(extractor.ColumnName);
            writer.WriteString(extractor.TypeName);

            // Custom serialization byte: server expects this for protocol >= 54454
            if (NegotiatedProtocolVersion >= Protocol.ProtocolVersion.WithCustomSerialization)
            {
                writer.WriteByte(0); // hasCustom = false
            }

            // Write column data directly - no intermediate arrays, no boxing
            if (rowCount > 0)
            {
                extractor.ExtractAndWrite(ref writer, rows, rowCount);
            }
        }
    }

    private void WriteDataBlockDirectCompressed<TRow>(
        ref ProtocolWriter writer,
        IReadOnlyList<IColumnExtractor<TRow>> extractors,
        IReadOnlyList<TRow> rows,
        int rowCount,
        ICompressor compressor)
    {
        // NOTE: Table name is now written by the caller (SendDataBlockDirectAsync) at the Data message level

        // Estimate buffer size for uncompressed data to avoid resize allocations
        var estimatedSize = (rowCount * extractors.Count * 32) + 1024;
        var uncompressedBuffer = BufferWriterPool.Shared.Rent(estimatedSize);

        try
        {
            var tempWriter = new ProtocolWriter(uncompressedBuffer);

            // Write block info
            BlockInfo.Default.Write(ref tempWriter);

            // Column count and row count
            tempWriter.WriteVarInt((ulong)extractors.Count);
            tempWriter.WriteVarInt((ulong)rowCount);

            // Write each column
            for (int i = 0; i < extractors.Count; i++)
            {
                var extractor = extractors[i];
                tempWriter.WriteString(extractor.ColumnName);
                tempWriter.WriteString(extractor.TypeName);

                // Custom serialization byte: server expects this for protocol >= 54454
                if (NegotiatedProtocolVersion >= Protocol.ProtocolVersion.WithCustomSerialization)
                {
                    tempWriter.WriteByte(0); // hasCustom = false
                }

                // Write column data
                if (rowCount > 0)
                {
                    extractor.ExtractAndWrite(ref tempWriter, rows, rowCount);
                }
            }

            // Compress and write (using pooled buffers to reduce GC pressure)
            using var compressed = CompressedBlock.CompressPooled(uncompressedBuffer.WrittenSpan, compressor);
            writer.WriteBytes(compressed.Span);
        }
        finally
        {
            BufferWriterPool.Shared.Return(uncompressedBuffer);
        }
    }

    /// <summary>
    /// Waits for the EndOfStream message from the server after INSERT completes.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    internal async Task ReceiveEndOfStreamAsync(CancellationToken cancellationToken)
    {
        var registry = ColumnReaderRegistry.Default;

        // Wire dump for debugging
        var wireDump = Environment.GetEnvironmentVariable("CH_WIRE_DUMP");
        if (wireDump == "1")
        {
            File.AppendAllText("/tmp/ch_wire_dump.log", "[CH] ReceiveEndOfStreamAsync: waiting for EndOfStream...\n");
        }

        while (true)
        {
            var result = await _pipeReader!.ReadAsync(cancellationToken);
            var buffer = result.Buffer;

            if (wireDump == "1")
            {
                File.AppendAllText("/tmp/ch_wire_dump.log", $"[CH] ReceiveEndOfStreamAsync: received {buffer.Length} bytes, IsCompleted={result.IsCompleted}\n");
            }

            if (result.IsCanceled)
                throw new OperationCanceledException(cancellationToken);

            if (buffer.IsEmpty && result.IsCompleted)
                throw new ClickHouseConnectionException("Server closed connection while waiting for INSERT completion");

            try
            {
                var reader = new ProtocolReader(buffer);
                var messageType = (ServerMessageType)reader.ReadVarInt();

                if (wireDump == "1")
                {
                    File.AppendAllText("/tmp/ch_wire_dump.log", $"[CH] ReceiveEndOfStreamAsync: messageType={messageType}\n");
                }

                switch (messageType)
                {
                    case ServerMessageType.EndOfStream:
                        _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                        return;

                    case ServerMessageType.Exception:
                        var exceptionMessage = ExceptionMessage.Read(ref reader);
                        _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                        throw ClickHouseServerException.FromExceptionMessage(exceptionMessage);

                    case ServerMessageType.Progress:
                        // Skip progress messages
                        ProgressMessage.Read(ref reader, NegotiatedProtocolVersion);
                        _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                        break;

                    case ServerMessageType.Data:
                        // Skip any data messages (e.g., empty confirmation blocks)
                        // For uncompressed data, do a scan pass first
                        if (!_compressionEnabled)
                        {
                            var scanBuffer = buffer.Slice(reader.Consumed);
                            if (!TryScanUncompressedDataMessage(scanBuffer, out _))
                            {
                                _pipeReader.AdvanceTo(buffer.Start, buffer.End);
                                continue;
                            }
                        }
                        ReadDataMessage(ref reader, registry);
                        _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                        break;

                    case ServerMessageType.ProfileInfo:
                        SkipProfileInfo(ref reader);
                        _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                        break;

                    case ServerMessageType.ProfileEvents:
                        // For uncompressed data, do a scan pass first
                        if (!_compressionEnabled)
                        {
                            var scanBuffer = buffer.Slice(reader.Consumed);
                            if (!TryScanUncompressedDataMessage(scanBuffer, out _))
                            {
                                _pipeReader.AdvanceTo(buffer.Start, buffer.End);
                                continue;
                            }
                        }
                        ReadDataMessage(ref reader, registry);
                        _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                        break;

                    case ServerMessageType.TableColumns:
                        // TableColumns message: external table name (string) + columns metadata (string)
                        // Skip both and continue waiting (matching clickhouse-cpp behavior)
                        reader.ReadString();
                        reader.ReadString();
                        _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                        break;

                    default:
                        throw new ClickHouseException($"Unexpected server message type during INSERT: {messageType}");
                }
            }
            catch (InvalidOperationException)
            {
                // Not enough data yet, need more
                _pipeReader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                    throw new ClickHouseConnectionException("Incomplete INSERT response from server");
            }
        }
    }

    #endregion
}
