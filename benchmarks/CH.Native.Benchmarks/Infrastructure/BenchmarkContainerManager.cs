using DotNet.Testcontainers.Builders;
using Testcontainers.ClickHouse;

namespace CH.Native.Benchmarks.Infrastructure;

/// <summary>
/// Singleton manager for ClickHouse container across all benchmarks.
/// Ensures container is started once and shared across benchmark iterations.
/// </summary>
public sealed class BenchmarkContainerManager : IAsyncDisposable
{
    private static readonly Lazy<BenchmarkContainerManager> _instance =
        new(() => new BenchmarkContainerManager());

    public static BenchmarkContainerManager Instance => _instance.Value;

    private ClickHouseContainer? _container;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;

    public string Host { get; private set; } = string.Empty;
    public int NativePort { get; private set; }
    public int HttpPort { get; private set; }
    public string Username { get; private set; } = "default";
    public string Password { get; private set; } = "benchmark_password";

    /// <summary>
    /// Connection string for CH.Native (native TCP protocol).
    /// </summary>
    public string NativeConnectionString =>
        $"Host={Host};Port={NativePort};Username={Username};Password={Password}";

    /// <summary>
    /// Connection string for ClickHouse.Driver (HTTP protocol).
    /// </summary>
    public string HttpConnectionString =>
        $"Host={Host};Port={HttpPort};Username={Username};Password={Password}";

    private BenchmarkContainerManager() { }

    public async Task EnsureInitializedAsync()
    {
        if (_initialized) return;

        await _initLock.WaitAsync();
        try
        {
            if (_initialized) return;

            _container = new ClickHouseBuilder()
                .WithImage("clickhouse/clickhouse-server:24.1")
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

            _initialized = true;

            Console.WriteLine($"[BenchmarkContainer] Started - Native: {Host}:{NativePort}, HTTP: {Host}:{HttpPort}");
        }
        finally
        {
            _initLock.Release();
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
