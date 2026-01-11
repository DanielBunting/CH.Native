using System.Security.Cryptography.X509Certificates;
using CH.Native.Compression;
using CH.Native.Resilience;
using CH.Native.Telemetry;
using Microsoft.Extensions.Logging;

namespace CH.Native.Connection;

/// <summary>
/// Builder for creating <see cref="ClickHouseConnectionSettings"/> instances.
/// </summary>
public sealed class ClickHouseConnectionSettingsBuilder
{
    private string _host = "localhost";
    private int _port = 9000;
    private string _database = "default";
    private string _username = "default";
    private string _password = "";
    private TimeSpan _connectTimeout = TimeSpan.FromSeconds(10);
    private int _receiveBufferSize = 262144;  // 256 KB - larger buffers reduce fragmentation
    private int _sendBufferSize = 131072;    // 128 KB
    private int _pipeBufferSize = 262144;    // 256 KB - PipeReader buffer for reduced segment boundaries
    private string _clientName = "CH.Native";
    private bool _compress = true;
    private CompressionMethod _compressionMethod = CompressionMethod.Lz4;
    private readonly List<ServerAddress> _servers = new();
    private LoadBalancingStrategy _loadBalancing = LoadBalancingStrategy.RoundRobin;
    private ResilienceOptions? _resilience;
    private bool _useTls = false;
    private int _tlsPort = 9440;
    private bool _allowInsecureTls = false;
    private string? _tlsCaCertificatePath;
    private X509Certificate2? _tlsClientCertificate;
    private TelemetrySettings? _telemetry;

    /// <summary>
    /// Sets the host name or IP address.
    /// </summary>
    /// <param name="host">The host name.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithHost(string host)
    {
        _host = host ?? throw new ArgumentNullException(nameof(host));
        return this;
    }

    /// <summary>
    /// Sets the port number.
    /// </summary>
    /// <param name="port">The port number (1-65535).</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithPort(int port)
    {
        if (port < 1 || port > 65535)
            throw new ArgumentOutOfRangeException(nameof(port), "Port must be between 1 and 65535.");
        _port = port;
        return this;
    }

    /// <summary>
    /// Sets the database name.
    /// </summary>
    /// <param name="database">The database name.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithDatabase(string database)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        return this;
    }

    /// <summary>
    /// Sets the username for authentication.
    /// </summary>
    /// <param name="username">The username.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithUsername(string username)
    {
        _username = username ?? throw new ArgumentNullException(nameof(username));
        return this;
    }

    /// <summary>
    /// Sets the password for authentication.
    /// </summary>
    /// <param name="password">The password.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithPassword(string password)
    {
        _password = password ?? "";
        return this;
    }

    /// <summary>
    /// Sets the username and password for authentication.
    /// </summary>
    /// <param name="username">The username.</param>
    /// <param name="password">The password.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithCredentials(string username, string password)
    {
        return WithUsername(username).WithPassword(password);
    }

    /// <summary>
    /// Sets the connection timeout.
    /// </summary>
    /// <param name="timeout">The timeout duration.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithConnectTimeout(TimeSpan timeout)
    {
        if (timeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout cannot be negative.");
        _connectTimeout = timeout;
        return this;
    }

    /// <summary>
    /// Sets the receive buffer size.
    /// </summary>
    /// <param name="size">The buffer size in bytes.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithReceiveBufferSize(int size)
    {
        if (size < 1)
            throw new ArgumentOutOfRangeException(nameof(size), "Buffer size must be positive.");
        _receiveBufferSize = size;
        return this;
    }

    /// <summary>
    /// Sets the send buffer size.
    /// </summary>
    /// <param name="size">The buffer size in bytes.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithSendBufferSize(int size)
    {
        if (size < 1)
            throw new ArgumentOutOfRangeException(nameof(size), "Buffer size must be positive.");
        _sendBufferSize = size;
        return this;
    }

    /// <summary>
    /// Sets the PipeReader buffer size. Larger buffers reduce memory fragmentation
    /// and improve parsing performance for large result sets.
    /// </summary>
    /// <param name="size">The buffer size in bytes (minimum 4096).</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithPipeBufferSize(int size)
    {
        if (size < 4096)
            throw new ArgumentOutOfRangeException(nameof(size), "Pipe buffer size must be at least 4096 bytes.");
        _pipeBufferSize = size;
        return this;
    }

    /// <summary>
    /// Sets the client name sent to the server.
    /// </summary>
    /// <param name="clientName">The client name.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithClientName(string clientName)
    {
        _clientName = clientName ?? throw new ArgumentNullException(nameof(clientName));
        return this;
    }

    /// <summary>
    /// Enables or disables compression for data transfer.
    /// </summary>
    /// <param name="enabled">True to enable compression, false to disable.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithCompression(bool enabled = true)
    {
        _compress = enabled;
        return this;
    }

    /// <summary>
    /// Sets the compression method to use when compression is enabled.
    /// </summary>
    /// <param name="method">The compression method.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithCompressionMethod(CompressionMethod method)
    {
        _compressionMethod = method;
        return this;
    }

    /// <summary>
    /// Adds a server to the list of servers for load balancing.
    /// </summary>
    /// <param name="host">The server host.</param>
    /// <param name="port">The server port (default 9000).</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithServer(string host, int port = 9000)
    {
        ArgumentNullException.ThrowIfNull(host);
        if (port < 1 || port > 65535)
            throw new ArgumentOutOfRangeException(nameof(port), "Port must be between 1 and 65535.");
        _servers.Add(new ServerAddress(host, port));
        return this;
    }

    /// <summary>
    /// Adds multiple servers to the list of servers for load balancing.
    /// </summary>
    /// <param name="servers">The server addresses to add.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithServers(params ServerAddress[] servers)
    {
        ArgumentNullException.ThrowIfNull(servers);
        _servers.AddRange(servers);
        return this;
    }

    /// <summary>
    /// Adds multiple servers to the list of servers for load balancing.
    /// </summary>
    /// <param name="servers">The server addresses to add.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithServers(IEnumerable<ServerAddress> servers)
    {
        ArgumentNullException.ThrowIfNull(servers);
        _servers.AddRange(servers);
        return this;
    }

    /// <summary>
    /// Sets the load balancing strategy for multi-server configurations.
    /// </summary>
    /// <param name="strategy">The load balancing strategy.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithLoadBalancing(LoadBalancingStrategy strategy)
    {
        _loadBalancing = strategy;
        return this;
    }

    /// <summary>
    /// Sets the resilience options (retry, circuit breaker, health check).
    /// </summary>
    /// <param name="options">The resilience options.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithResilience(ResilienceOptions options)
    {
        _resilience = options;
        return this;
    }

    /// <summary>
    /// Configures resilience options using a callback.
    /// </summary>
    /// <param name="configure">A callback to configure the resilience options.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithResilience(Action<ResilienceOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new ResilienceOptionsBuilder();
        configure(builder);
        _resilience = builder.Build();
        return this;
    }

    /// <summary>
    /// Enables or disables TLS for the connection.
    /// When enabled, connects to the TLS port (default 9440).
    /// </summary>
    /// <param name="enabled">True to enable TLS, false to disable.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithTls(bool enabled = true)
    {
        _useTls = enabled;
        return this;
    }

    /// <summary>
    /// Sets the TLS port to connect to when TLS is enabled.
    /// Default is 9440 (ClickHouse secure native port).
    /// </summary>
    /// <param name="port">The TLS port number (1-65535).</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithTlsPort(int port)
    {
        if (port < 1 || port > 65535)
            throw new ArgumentOutOfRangeException(nameof(port), "TLS port must be between 1 and 65535.");
        _tlsPort = port;
        return this;
    }

    /// <summary>
    /// Allows insecure TLS connections that skip certificate validation.
    /// WARNING: Only use this for testing with self-signed certificates.
    /// </summary>
    /// <param name="allow">True to skip certificate validation, false for normal validation.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithAllowInsecureTls(bool allow = true)
    {
        _allowInsecureTls = allow;
        return this;
    }

    /// <summary>
    /// Sets a custom CA certificate file for TLS validation.
    /// Use this when connecting to servers with certificates signed by a private CA.
    /// </summary>
    /// <param name="path">Path to the CA certificate file (PEM or CRT format).</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithTlsCaCertificate(string path)
    {
        _tlsCaCertificatePath = path ?? throw new ArgumentNullException(nameof(path));
        return this;
    }

    /// <summary>
    /// Sets a client certificate for mutual TLS (mTLS) authentication.
    /// </summary>
    /// <param name="certificate">The client certificate.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithTlsClientCertificate(X509Certificate2 certificate)
    {
        _tlsClientCertificate = certificate ?? throw new ArgumentNullException(nameof(certificate));
        return this;
    }

    /// <summary>
    /// Sets the telemetry settings (tracing, metrics, logging).
    /// </summary>
    /// <param name="settings">The telemetry settings.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithTelemetry(TelemetrySettings settings)
    {
        _telemetry = settings ?? throw new ArgumentNullException(nameof(settings));
        return this;
    }

    /// <summary>
    /// Sets the logger factory for telemetry logging.
    /// This is a convenience method that creates or updates TelemetrySettings with the specified logger factory.
    /// </summary>
    /// <param name="loggerFactory">The logger factory.</param>
    /// <returns>This builder for chaining.</returns>
    public ClickHouseConnectionSettingsBuilder WithLoggerFactory(ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _telemetry = (_telemetry ?? TelemetrySettings.Default) with { LoggerFactory = loggerFactory };
        return this;
    }

    /// <summary>
    /// Builds the connection settings.
    /// </summary>
    /// <returns>The built settings.</returns>
    /// <exception cref="InvalidOperationException">Thrown if required settings are missing.</exception>
    public ClickHouseConnectionSettings Build()
    {
        if (string.IsNullOrWhiteSpace(_host))
            throw new InvalidOperationException("Host is required.");

        return new ClickHouseConnectionSettings(
            _host,
            _port,
            _database,
            _username,
            _password,
            _connectTimeout,
            _receiveBufferSize,
            _sendBufferSize,
            _pipeBufferSize,
            _clientName,
            _compress,
            _compressionMethod,
            _servers.Count > 0 ? _servers.ToList() : null,
            _loadBalancing,
            _resilience,
            _useTls,
            _tlsPort,
            _allowInsecureTls,
            _tlsCaCertificatePath,
            _tlsClientCertificate,
            _telemetry);
    }
}

/// <summary>
/// Builder for creating <see cref="ResilienceOptions"/> instances.
/// </summary>
public sealed class ResilienceOptionsBuilder
{
    private RetryOptions? _retry;
    private CircuitBreakerOptions? _circuitBreaker;
    private TimeSpan _healthCheckInterval = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Enables retry with the specified options.
    /// </summary>
    /// <param name="options">The retry options.</param>
    /// <returns>This builder for chaining.</returns>
    public ResilienceOptionsBuilder WithRetry(RetryOptions options)
    {
        _retry = options;
        return this;
    }

    /// <summary>
    /// Enables retry with default options.
    /// </summary>
    /// <returns>This builder for chaining.</returns>
    public ResilienceOptionsBuilder WithRetry()
    {
        _retry = RetryOptions.Default;
        return this;
    }

    /// <summary>
    /// Enables circuit breaker with the specified options.
    /// </summary>
    /// <param name="options">The circuit breaker options.</param>
    /// <returns>This builder for chaining.</returns>
    public ResilienceOptionsBuilder WithCircuitBreaker(CircuitBreakerOptions options)
    {
        _circuitBreaker = options;
        return this;
    }

    /// <summary>
    /// Enables circuit breaker with default options.
    /// </summary>
    /// <returns>This builder for chaining.</returns>
    public ResilienceOptionsBuilder WithCircuitBreaker()
    {
        _circuitBreaker = CircuitBreakerOptions.Default;
        return this;
    }

    /// <summary>
    /// Sets the health check interval.
    /// </summary>
    /// <param name="interval">The interval between health checks.</param>
    /// <returns>This builder for chaining.</returns>
    public ResilienceOptionsBuilder WithHealthCheckInterval(TimeSpan interval)
    {
        _healthCheckInterval = interval;
        return this;
    }

    /// <summary>
    /// Builds the resilience options.
    /// </summary>
    /// <returns>The built resilience options.</returns>
    public ResilienceOptions Build()
    {
        return new ResilienceOptions
        {
            Retry = _retry,
            CircuitBreaker = _circuitBreaker,
            HealthCheckInterval = _healthCheckInterval
        };
    }
}
