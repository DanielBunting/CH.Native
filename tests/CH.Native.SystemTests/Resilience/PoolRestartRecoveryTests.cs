using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pool behaviour after a real server restart — distinct from network-chaos coverage
/// because a container restart actually loses session state and changes the
/// connection-side observation order. Existing tests rely on Toxiproxy to simulate
/// network damage; only a real stop/start exercises stale-socket eviction and
/// server-side session loss together.
///
/// <para>Uses <see cref="RestartableSingleNodeFixture"/> rather than the default
/// <see cref="SingleNodeFixture"/> because Testcontainers' default random-port
/// allocation does not survive <c>StopAsync</c>/<c>StartAsync</c> (the host port
/// gets reassigned). The restartable fixture pins a fixed host-port mapping so
/// already-constructed <see cref="ClickHouseDataSource"/> instances stay valid
/// across restarts — which is the whole point of these tests.</para>
///
/// <para>This class mutates the shared fixture's container lifecycle.
/// <see cref="DisposeAsync"/> always restarts the container so other classes
/// in the <c>RestartableSingleNode</c> collection see it healthy after these
/// tests run.</para>
/// </summary>
[Collection("RestartableSingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public sealed class PoolRestartRecoveryTests : IAsyncLifetime
{
    private readonly RestartableSingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public PoolRestartRecoveryTests(RestartableSingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        // Belt-and-braces: even if a test throws between Stop and Start, the next
        // class in the SingleNode collection must see a running container.
        try
        {
            await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
            await conn.OpenAsync();
        }
        catch
        {
            try { await _fixture.StartContainerAsync(); } catch { }
        }
    }

    private ClickHouseDataSource BuildDataSource(int maxPoolSize = 4) =>
        new(new ClickHouseDataSourceOptions
        {
            Settings = _fixture.BuildSettings(),
            MaxPoolSize = maxPoolSize,
            ValidateOnRent = true,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(15),
        });

    [Fact]
    public async Task RentReturnRestartRent_StaleEntriesDiscarded_FreshOpenSucceeds()
    {
        await using var ds = BuildDataSource();

        // Establish at least one physical connection in the idle pool.
        await using (var conn = await ds.OpenConnectionAsync())
        {
            Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
        }

        var preStats = ds.GetStatistics();
        _output.WriteLine($"Pre-restart stats: {preStats}");
        Assert.True(preStats.TotalCreated >= 1);
        Assert.True(preStats.Idle >= 1);

        await _fixture.StopContainerAsync();
        await _fixture.StartContainerAsync();

        // The idle entry from before the restart points to a dead session. The pool
        // must detect this on rent (ValidateOnRent or first I/O) and create a fresh socket.
        await using (var fresh = await ds.OpenConnectionAsync())
        {
            Assert.Equal(2, await fresh.ExecuteScalarAsync<int>("SELECT 2"));
        }

        var postStats = ds.GetStatistics();
        _output.WriteLine($"Post-restart stats: {postStats}");

        // A fresh physical connection had to be created — TotalCreated grew, and the
        // stale one was evicted rather than silently returned to idle.
        Assert.True(postStats.TotalCreated > preStats.TotalCreated,
            $"Expected a new physical connection after restart; TotalCreated went {preStats.TotalCreated} → {postStats.TotalCreated}.");
        Assert.True(postStats.TotalEvicted >= preStats.TotalEvicted + 1,
            $"Expected the stale connection to be evicted; TotalEvicted went {preStats.TotalEvicted} → {postStats.TotalEvicted}.");
    }

    [Fact]
    public async Task ActiveReaderInterruptedByRestart_FailsCleanly_NextRentOpensFreshSocket()
    {
        await using var ds = BuildDataSource();

        var conn = await ds.OpenConnectionAsync();

        // Long-running stream — needs to be in flight when the container goes away.
        var readerTask = Task.Run(async () =>
        {
            int rowsSeen = 0;
            try
            {
                await foreach (var _ in conn.QueryAsync<ulong>(
                    "SELECT number FROM numbers(50_000_000)"))
                {
                    rowsSeen++;
                }
            }
            catch (Exception ex)
            {
                return (rowsSeen, error: (Exception?)ex);
            }
            return (rowsSeen, error: null);
        });

        // Give the reader a beat to actually start streaming, then yank the container.
        await Task.Delay(300);
        await _fixture.StopContainerAsync();

        var (consumed, error) = await readerTask;
        _output.WriteLine($"Reader saw {consumed} rows before restart; error: {error?.GetType().FullName}");
        Assert.NotNull(error);

        // Always restore the container before any further assertion, so a flaky
        // restart-time failure does not cascade.
        await _fixture.StartContainerAsync();

        // Discard the poisoned rent so its slot frees up — the inner connection
        // is dead and its dispose path will close cleanly.
        await conn.DisposeAsync();

        // A fresh rent from the same data source must succeed against the restored container.
        await using var fresh = await ds.OpenConnectionAsync();
        Assert.Equal(7, await fresh.ExecuteScalarAsync<int>("SELECT 7"));

        var stats = ds.GetStatistics();
        _output.WriteLine($"Post-recovery stats: {stats}");
        Assert.Equal(0, stats.PendingWaits);
    }

    [Fact]
    public async Task ConcurrentBurstThroughRestart_StatisticsSettleClean()
    {
        await using var ds = BuildDataSource(maxPoolSize: 6);

        // Warm the pool with a couple of physical connections so there's idle state to evict.
        var warm = await Task.WhenAll(
            Enumerable.Range(0, 3).Select(_ => ds.OpenConnectionAsync().AsTask()));
        foreach (var c in warm)
        {
            await c.ExecuteScalarAsync<int>("SELECT 1");
            await c.DisposeAsync();
        }

        var preStats = ds.GetStatistics();
        _output.WriteLine($"Pre-burst stats: {preStats}");

        // Kick off a burst of work; restart partway through.
        var cts = new CancellationTokenSource();
        var burst = Task.WhenAll(Enumerable.Range(0, 12).Select(async i =>
        {
            try
            {
                await using var c = await ds.OpenConnectionAsync(cts.Token);
                await c.ExecuteScalarAsync<int>($"SELECT {i}");
            }
            catch
            {
                // Expected for the in-flight rents at restart time.
            }
        }));

        await Task.Delay(150);
        await _fixture.StopContainerAsync();
        await _fixture.StartContainerAsync();

        cts.Cancel();
        await burst;

        // After settle, no waiters and no busy connections should remain.
        // Allow the pool a brief moment to absorb returns before sampling stats.
        await Task.Delay(200);
        var postStats = ds.GetStatistics();
        _output.WriteLine($"Post-burst stats: {postStats}");

        Assert.Equal(0, postStats.Busy);
        Assert.Equal(0, postStats.PendingWaits);
        Assert.True(postStats.TotalEvicted >= preStats.TotalEvicted,
            "Restart should have evicted at least the warm-pool entries.");

        // Sanity: a final fresh rent works.
        await using var final = await ds.OpenConnectionAsync();
        Assert.Equal(99, await final.ExecuteScalarAsync<int>("SELECT 99"));
    }
}
