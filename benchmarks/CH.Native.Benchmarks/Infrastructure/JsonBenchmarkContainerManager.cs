using DotNet.Testcontainers.Builders;
using Testcontainers.ClickHouse;

namespace CH.Native.Benchmarks.Infrastructure;

/// <summary>
/// Singleton manager for ClickHouse 25.6+ container for JSON benchmarks.
/// JSON type requires ClickHouse 25.6 or later with flattened serialization support.
/// </summary>
public sealed class JsonBenchmarkContainerManager : IAsyncDisposable
{
    private static readonly Lazy<JsonBenchmarkContainerManager> _instance =
        new(() => new JsonBenchmarkContainerManager());

    public static JsonBenchmarkContainerManager Instance => _instance.Value;

    private ClickHouseContainer? _container;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;

    public string Host { get; private set; } = string.Empty;
    public int NativePort { get; private set; }
    public int HttpPort { get; private set; }
    public string Username { get; private set; } = "default";
    public string Password { get; private set; } = "json_benchmark_password";

    /// <summary>
    /// Gets the ClickHouse server version (major.minor).
    /// </summary>
    public Version? ServerVersion { get; private set; }

    /// <summary>
    /// Connection string for CH.Native (native TCP protocol).
    /// </summary>
    public string NativeConnectionString =>
        $"Host={Host};Port={NativePort};Username={Username};Password={Password}";

    /// <summary>
    /// Connection string for ClickHouse.Client (HTTP protocol).
    /// </summary>
    public string HttpConnectionString =>
        $"Host={Host};Port={HttpPort};Username={Username};Password={Password}";

    /// <summary>
    /// Indicates whether the server supports JSON type (25.6+).
    /// </summary>
    public bool SupportsJson => ServerVersion != null &&
        (ServerVersion.Major > 25 || (ServerVersion.Major == 25 && ServerVersion.Minor >= 6));

    private JsonBenchmarkContainerManager() { }

    public async Task EnsureInitializedAsync()
    {
        if (_initialized) return;

        await _initLock.WaitAsync();
        try
        {
            if (_initialized) return;

            // Use ClickHouse 25.6+ for JSON support
            // Note: Using 'latest' tag to get the newest version with JSON support
            // In production benchmarks, pin to a specific version like "25.6" or "25.8"
            _container = new ClickHouseBuilder()
                .WithImage("clickhouse/clickhouse-server:latest")
                .WithUsername(Username)
                .WithPassword(Password)
                .WithPortBinding(9000, true)
                .WithPortBinding(8123, true)
                .WithWaitStrategy(Wait.ForUnixContainer()
                    .UntilPortIsAvailable(9000)
                    .UntilPortIsAvailable(8123))
                .Build();

            await _container.StartAsync();

            Host = _container.Hostname;
            NativePort = _container.GetMappedPublicPort(9000);
            HttpPort = _container.GetMappedPublicPort(8123);

            // Detect server version
            await DetectServerVersionAsync();

            _initialized = true;

            Console.WriteLine($"[JsonBenchmarkContainer] Started - Version: {ServerVersion}, Native: {Host}:{NativePort}, HTTP: {Host}:{HttpPort}");
            Console.WriteLine($"[JsonBenchmarkContainer] JSON Support: {SupportsJson}");
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task DetectServerVersionAsync()
    {
        // Retry logic: container ports may be available but server not fully ready
        const int maxRetries = 10;
        const int delayMs = 500;

        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                await using var connection = new CH.Native.Connection.ClickHouseConnection(NativeConnectionString);
                await connection.OpenAsync();

                var serverInfo = connection.ServerInfo;
                if (serverInfo != null)
                {
                    ServerVersion = new Version(serverInfo.VersionMajor, serverInfo.VersionMinor);
                    return;
                }
            }
            catch (Exception ex)
            {
                if (attempt == maxRetries)
                {
                    Console.WriteLine($"[JsonBenchmarkContainer] Warning: Could not detect server version after {maxRetries} attempts: {ex.Message}");
                }
                else
                {
                    await Task.Delay(delayMs);
                }
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_container != null)
        {
            await _container.DisposeAsync();
            _container = null;
        }
        _initLock.Dispose();
    }
}
