using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Testcontainers.ClickHouse;
using Xunit;

namespace CH.Native.SmokeTests.Fixtures;

public class SmokeTestFixture : IAsyncLifetime
{
    private const string TestUsername = "default";
    private const string TestPassword = "smoke_password";

    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.11")
        .WithUsername(TestUsername)
        .WithPassword(TestPassword)
        .WithPortBinding(9000, true)
        .WithPortBinding(8123, true)
        .WithWaitStrategy(Wait.ForUnixContainer()
            .UntilPortIsAvailable(9000)
            .UntilPortIsAvailable(8123))
        .Build();

    public string Host => _container.Hostname;
    public int NativePort => _container.GetMappedPublicPort(9000);
    public int HttpPort => _container.GetMappedPublicPort(8123);
    public string Username => TestUsername;
    public string Password => TestPassword;

    public string NativeConnectionString =>
        $"Host={Host};Port={NativePort};Username={Username};Password={Password}";

    public string DriverConnectionString =>
        $"Host={Host};Port={HttpPort};Username={Username};Password={Password}";

    public string NativeConnectionStringWithCompression(string method) =>
        $"Host={Host};Port={NativePort};Username={Username};Password={Password};Compression=true;CompressionMethod={method}";

    public string NativeLazyConnectionString =>
        $"Host={Host};Port={NativePort};Username={Username};Password={Password};StringMaterialization=Lazy";

    /// <summary>
    /// Executes clickhouse-client inside the server container (connects to localhost:9000).
    /// argv-style exec: no shell, no quoting concerns, no stdin support in Testcontainers 4.x —
    /// inserts must inline VALUES in the query. Per-query settings go via
    /// <paramref name="extraArgs"/> (e.g. "--enable_time_time64_type=1") since each exec is a
    /// fresh session.
    /// </summary>
    public async Task<ExecResult> ExecClickHouseClientAsync(
        string query,
        string format = "TabSeparated",
        IEnumerable<string>? extraArgs = null)
    {
        var argv = new List<string>
        {
            "clickhouse-client",
            "--user", Username,
            "--password", Password,
            "--format", format,
        };
        if (extraArgs is not null)
        {
            argv.AddRange(extraArgs);
        }
        argv.Add("--query");
        argv.Add(query);
        return await _container.ExecAsync(argv);
    }

    public async Task InitializeAsync()
    {
        await _container.StartAsync();

        // Wait for server to be fully ready
        for (int attempt = 1; attempt <= 20; attempt++)
        {
            try
            {
                await using var connection = new Connection.ClickHouseConnection(NativeConnectionString);
                await connection.OpenAsync();
                return;
            }
            catch
            {
                if (attempt == 20) throw;
                await Task.Delay(500);
            }
        }
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}

[CollectionDefinition("SmokeTest")]
public class SmokeTestCollection : ICollectionFixture<SmokeTestFixture>
{
}
