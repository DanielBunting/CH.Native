using System.Security.Cryptography.X509Certificates;
using CH.Native.Compression;
using CH.Native.Data;
using CH.Native.Resilience;
using CH.Native.Telemetry;

namespace CH.Native.Connection;

/// <summary>
/// Immutable settings for a ClickHouse connection.
/// </summary>
public sealed class ClickHouseConnectionSettings
{
    /// <summary>
    /// Gets the host name or IP address of the ClickHouse server.
    /// </summary>
    public string Host { get; }

    /// <summary>
    /// Gets the native protocol port (default 9000).
    /// </summary>
    public int Port { get; }

    /// <summary>
    /// Gets the database to connect to.
    /// </summary>
    public string Database { get; }

    /// <summary>
    /// Gets the username for authentication.
    /// </summary>
    public string Username { get; }

    /// <summary>
    /// Gets the password for authentication.
    /// </summary>
    public string Password { get; }

    /// <summary>
    /// Gets the connection timeout.
    /// </summary>
    public TimeSpan ConnectTimeout { get; }

    /// <summary>
    /// Gets the receive buffer size in bytes.
    /// </summary>
    public int ReceiveBufferSize { get; }

    /// <summary>
    /// Gets the send buffer size in bytes.
    /// </summary>
    public int SendBufferSize { get; }

    /// <summary>
    /// Gets the PipeReader buffer size in bytes.
    /// Larger buffers reduce memory fragmentation and improve parsing performance.
    /// </summary>
    public int PipeBufferSize { get; }

    /// <summary>
    /// Gets the client name sent to the server.
    /// </summary>
    public string ClientName { get; }

    /// <summary>
    /// Gets whether compression is enabled for data transfer.
    /// </summary>
    public bool Compress { get; }

    /// <summary>
    /// Gets the compression method to use when compression is enabled.
    /// </summary>
    public CompressionMethod CompressionMethod { get; }

    /// <summary>
    /// Gets the list of server addresses for multi-server configurations.
    /// If not specified, defaults to a single server using Host and Port.
    /// </summary>
    public IReadOnlyList<ServerAddress> Servers { get; }

    /// <summary>
    /// Gets the load balancing strategy for multi-server configurations.
    /// </summary>
    public LoadBalancingStrategy LoadBalancing { get; }

    /// <summary>
    /// Gets the resilience options (retry, circuit breaker, health check interval).
    /// </summary>
    public ResilienceOptions? Resilience { get; }

    /// <summary>
    /// Gets whether TLS is enabled for the connection.
    /// </summary>
    public bool UseTls { get; }

    /// <summary>
    /// Gets the TLS port (default 9440).
    /// </summary>
    public int TlsPort { get; }

    /// <summary>
    /// Gets whether to allow insecure TLS connections (skip certificate validation).
    /// Only use for testing with self-signed certificates.
    /// </summary>
    public bool AllowInsecureTls { get; }

    /// <summary>
    /// Gets the path to a custom CA certificate file for TLS validation.
    /// </summary>
    public string? TlsCaCertificatePath { get; }

    /// <summary>
    /// Gets the client certificate for mutual TLS (mTLS) authentication.
    /// </summary>
    public X509Certificate2? TlsClientCertificate { get; }

    /// <summary>
    /// Gets the telemetry settings (tracing, metrics, logging).
    /// </summary>
    public TelemetrySettings? Telemetry { get; }

    /// <summary>
    /// Gets the string materialization strategy.
    /// </summary>
    public StringMaterialization StringMaterialization { get; }

    /// <summary>
    /// Gets the effective port to use for the connection (TlsPort if UseTls, otherwise Port).
    /// </summary>
    public int EffectivePort => UseTls ? TlsPort : Port;

    /// <summary>
    /// Creates connection settings with the specified values.
    /// </summary>
    internal ClickHouseConnectionSettings(
        string host,
        int port,
        string database,
        string username,
        string password,
        TimeSpan connectTimeout,
        int receiveBufferSize,
        int sendBufferSize,
        int pipeBufferSize,
        string clientName,
        bool compress,
        CompressionMethod compressionMethod,
        IReadOnlyList<ServerAddress>? servers,
        LoadBalancingStrategy loadBalancing,
        ResilienceOptions? resilience,
        bool useTls,
        int tlsPort,
        bool allowInsecureTls,
        string? tlsCaCertificatePath,
        X509Certificate2? tlsClientCertificate,
        TelemetrySettings? telemetry,
        StringMaterialization stringMaterialization)
    {
        Host = host;
        Port = port;
        Database = database;
        Username = username;
        Password = password;
        ConnectTimeout = connectTimeout;
        ReceiveBufferSize = receiveBufferSize;
        SendBufferSize = sendBufferSize;
        PipeBufferSize = pipeBufferSize;
        ClientName = clientName;
        Compress = compress;
        CompressionMethod = compressionMethod;

        // If servers not specified, create a single-server list from Host/Port
        Servers = servers is { Count: > 0 }
            ? servers
            : [new ServerAddress(host, port)];
        LoadBalancing = loadBalancing;
        Resilience = resilience;

        // TLS settings
        UseTls = useTls;
        TlsPort = tlsPort;
        AllowInsecureTls = allowInsecureTls;
        TlsCaCertificatePath = tlsCaCertificatePath;
        TlsClientCertificate = tlsClientCertificate;

        // Telemetry settings
        Telemetry = telemetry;

        // String materialization
        StringMaterialization = stringMaterialization;
    }

    /// <summary>
    /// Parses a connection string into settings.
    /// </summary>
    /// <param name="connectionString">
    /// Connection string in format: "Host=localhost;Port=9000;Database=default;Username=default;Password="
    /// </param>
    /// <returns>The parsed settings.</returns>
    /// <exception cref="ArgumentException">Thrown if the connection string is invalid.</exception>
    public static ClickHouseConnectionSettings Parse(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string cannot be null or empty.", nameof(connectionString));

        var builder = CreateBuilder();
        var pairs = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var hasHost = false;

        // Track resilience options parsed from connection string
        int? maxRetries = null;
        int? retryBaseDelayMs = null;
        int? retryMaxDelayMs = null;
        int? circuitBreakerThreshold = null;
        int? circuitBreakerDurationSec = null;
        int? healthCheckIntervalSec = null;

        foreach (var pair in pairs)
        {
            var equalsIndex = pair.IndexOf('=');
            if (equalsIndex < 0)
                continue;

            var key = pair[..equalsIndex].Trim();
            var value = pair[(equalsIndex + 1)..].Trim();

            switch (key.ToLowerInvariant())
            {
                case "host":
                case "server":
                    builder.WithHost(value);
                    hasHost = true;
                    break;
                case "port":
                    if (!int.TryParse(value, out var port) || port < 1 || port > 65535)
                        throw new ArgumentException($"Invalid port value: {value}", nameof(connectionString));
                    builder.WithPort(port);
                    break;
                case "database":
                case "db":
                    builder.WithDatabase(value);
                    break;
                case "username":
                case "user":
                case "uid":
                    builder.WithUsername(value);
                    break;
                case "password":
                case "pwd":
                    builder.WithPassword(value);
                    break;
                case "timeout":
                case "connecttimeout":
                    if (!int.TryParse(value, out var timeout) || timeout < 0)
                        throw new ArgumentException($"Invalid timeout value: {value}", nameof(connectionString));
                    builder.WithConnectTimeout(TimeSpan.FromSeconds(timeout));
                    break;
                case "compress":
                case "compression":
                    if (bool.TryParse(value, out var compress))
                    {
                        builder.WithCompression(compress);
                    }
                    else if (value.Equals("1", StringComparison.Ordinal))
                    {
                        builder.WithCompression(true);
                    }
                    else if (value.Equals("0", StringComparison.Ordinal))
                    {
                        builder.WithCompression(false);
                    }
                    else
                    {
                        throw new ArgumentException($"Invalid compression value: {value}", nameof(connectionString));
                    }
                    break;
                case "compressionmethod":
                    if (!Enum.TryParse<CompressionMethod>(value, ignoreCase: true, out var method))
                        throw new ArgumentException($"Invalid compression method: {value}. Valid values: Lz4, Zstd", nameof(connectionString));
                    builder.WithCompressionMethod(method);
                    break;
                case "servers":
                    var serverList = value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                    foreach (var server in serverList)
                    {
                        var serverAddress = ServerAddress.Parse(server);
                        builder.WithServer(serverAddress.Host, serverAddress.Port);
                    }
                    break;
                case "loadbalancing":
                case "loadbalancer":
                    if (!Enum.TryParse<LoadBalancingStrategy>(value, ignoreCase: true, out var strategy))
                        throw new ArgumentException($"Invalid load balancing strategy: {value}. Valid values: RoundRobin, Random, FirstAvailable", nameof(connectionString));
                    builder.WithLoadBalancing(strategy);
                    break;

                // Resilience options
                case "maxretries":
                case "retries":
                    if (!int.TryParse(value, out var retries) || retries < 0)
                        throw new ArgumentException($"Invalid max retries value: {value}", nameof(connectionString));
                    maxRetries = retries;
                    break;
                case "retrybasedelay":
                case "retrydelay":
                    if (!int.TryParse(value, out var baseDelay) || baseDelay < 0)
                        throw new ArgumentException($"Invalid retry base delay value: {value}", nameof(connectionString));
                    retryBaseDelayMs = baseDelay;
                    break;
                case "retrymaxdelay":
                    if (!int.TryParse(value, out var maxDelay) || maxDelay < 0)
                        throw new ArgumentException($"Invalid retry max delay value: {value}", nameof(connectionString));
                    retryMaxDelayMs = maxDelay;
                    break;
                case "circuitbreakerthreshold":
                case "cbthreshold":
                    if (!int.TryParse(value, out var threshold) || threshold < 1)
                        throw new ArgumentException($"Invalid circuit breaker threshold value: {value}", nameof(connectionString));
                    circuitBreakerThreshold = threshold;
                    break;
                case "circuitbreakerduration":
                case "cbduration":
                    if (!int.TryParse(value, out var duration) || duration < 1)
                        throw new ArgumentException($"Invalid circuit breaker duration value: {value}", nameof(connectionString));
                    circuitBreakerDurationSec = duration;
                    break;
                case "healthcheckinterval":
                    if (!int.TryParse(value, out var hcInterval) || hcInterval < 1)
                        throw new ArgumentException($"Invalid health check interval value: {value}", nameof(connectionString));
                    healthCheckIntervalSec = hcInterval;
                    break;

                // TLS options
                case "usetls":
                case "tls":
                case "ssl":
                case "secure":
                    if (bool.TryParse(value, out var useTls))
                    {
                        builder.WithTls(useTls);
                    }
                    else if (value.Equals("1", StringComparison.Ordinal))
                    {
                        builder.WithTls(true);
                    }
                    else if (value.Equals("0", StringComparison.Ordinal))
                    {
                        builder.WithTls(false);
                    }
                    else
                    {
                        throw new ArgumentException($"Invalid TLS value: {value}", nameof(connectionString));
                    }
                    break;
                case "tlsport":
                case "sslport":
                case "secureport":
                    if (!int.TryParse(value, out var tlsPort) || tlsPort < 1 || tlsPort > 65535)
                        throw new ArgumentException($"Invalid TLS port value: {value}", nameof(connectionString));
                    builder.WithTlsPort(tlsPort);
                    break;
                case "allowinsecuretls":
                case "trustservercertificate":
                case "insecure":
                    if (bool.TryParse(value, out var allowInsecure))
                    {
                        builder.WithAllowInsecureTls(allowInsecure);
                    }
                    else if (value.Equals("1", StringComparison.Ordinal))
                    {
                        builder.WithAllowInsecureTls(true);
                    }
                    else if (value.Equals("0", StringComparison.Ordinal))
                    {
                        builder.WithAllowInsecureTls(false);
                    }
                    else
                    {
                        throw new ArgumentException($"Invalid AllowInsecureTls value: {value}", nameof(connectionString));
                    }
                    break;
                case "tlscacertificate":
                case "sslca":
                case "cacert":
                    builder.WithTlsCaCertificate(value);
                    break;

                case "stringmaterialization":
                    if (!Enum.TryParse<StringMaterialization>(value, ignoreCase: true, out var materialization))
                        throw new ArgumentException($"Invalid string materialization value: {value}. Valid values: Eager, Lazy", nameof(connectionString));
                    builder.WithStringMaterialization(materialization);
                    break;
            }
        }

        if (!hasHost)
            throw new InvalidOperationException("Host is required in the connection string.");

        // Build resilience options if any were specified
        if (maxRetries.HasValue || retryBaseDelayMs.HasValue || circuitBreakerThreshold.HasValue || healthCheckIntervalSec.HasValue)
        {
            RetryOptions? retryOptions = null;
            if (maxRetries.HasValue || retryBaseDelayMs.HasValue || retryMaxDelayMs.HasValue)
            {
                retryOptions = new RetryOptions
                {
                    MaxRetries = maxRetries ?? 3,
                    BaseDelay = TimeSpan.FromMilliseconds(retryBaseDelayMs ?? 100),
                    MaxDelay = TimeSpan.FromMilliseconds(retryMaxDelayMs ?? 30000)
                };
            }

            CircuitBreakerOptions? circuitBreakerOptions = null;
            if (circuitBreakerThreshold.HasValue || circuitBreakerDurationSec.HasValue)
            {
                circuitBreakerOptions = new CircuitBreakerOptions
                {
                    FailureThreshold = circuitBreakerThreshold ?? 5,
                    OpenDuration = TimeSpan.FromSeconds(circuitBreakerDurationSec ?? 30)
                };
            }

            builder.WithResilience(new ResilienceOptions
            {
                Retry = retryOptions,
                CircuitBreaker = circuitBreakerOptions,
                HealthCheckInterval = TimeSpan.FromSeconds(healthCheckIntervalSec ?? 10)
            });
        }

        return builder.Build();
    }

    /// <summary>
    /// Creates a new builder for fluent configuration.
    /// </summary>
    /// <returns>A new settings builder.</returns>
    public static ClickHouseConnectionSettingsBuilder CreateBuilder() => new();

    /// <summary>
    /// Returns the connection string representation (without password).
    /// </summary>
    public override string ToString()
    {
        var str = $"Host={Host};Port={Port};Database={Database};Username={Username}";
        if (UseTls)
        {
            str += $";UseTls=true;TlsPort={TlsPort}";
            if (AllowInsecureTls)
                str += ";AllowInsecureTls=true";
        }
        return str;
    }
}
