using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Probes pool / reader / dispose behaviour when the server is hard-killed
/// (<c>docker kill --signal=KILL</c>) rather than stopped gracefully. Distinct from
/// <see cref="ServerRestartProbeTests"/> (graceful stop) and from network chaos
/// (toxiproxy reset_peer): a SIGKILL to PID 1 means the kernel never sends FIN —
/// the client sees an abrupt connection reset on its next syscall.
/// </summary>
[Collection("RestartableSingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public sealed class ServerKillProbeTests : IAsyncLifetime
{
    private readonly RestartableSingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ServerKillProbeTests(RestartableSingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        // Always restore the container so the next class in the collection sees it healthy.
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
            ConnectionWaitTimeout = TimeSpan.FromSeconds(10),
        });

    [Fact]
    public async Task KillContainer_DuringIdlePool_StaleSocketsEvictedOnNextRent()
    {
        await using var ds = BuildDataSource();

        // Warm two idle connections.
        var warm = await Task.WhenAll(
            Enumerable.Range(0, 2).Select(_ => ds.OpenConnectionAsync().AsTask()));
        foreach (var c in warm)
        {
            _ = await c.ExecuteScalarAsync<int>("SELECT 1");
            await c.DisposeAsync();
        }

        var pre = ds.GetStatistics();
        _output.WriteLine($"Pre-kill: {pre}");
        Assert.True(pre.Idle >= 1);

        await _fixture.KillContainerAsync();
        await _fixture.StartContainerAsync();

        // Next rent must produce a fresh physical connection.
        await using (var fresh = await ds.OpenConnectionAsync())
        {
            Assert.Equal(1, await fresh.ExecuteScalarAsync<int>("SELECT 1"));
        }

        var post = ds.GetStatistics();
        _output.WriteLine($"Post-kill: {post}");
        Assert.True(post.TotalCreated > pre.TotalCreated,
            "SIGKILL must invalidate stale idle entries — fresh creates should follow");
    }

    [Fact]
    public async Task KillContainer_DuringActiveReader_FailsCleanly_PoolRecovers()
    {
        await using var ds = BuildDataSource();

        var conn = await ds.OpenConnectionAsync();
        var readerTask = Task.Run(async () =>
        {
            int rows = 0;
            try
            {
                await foreach (var _ in conn.QueryAsync<ulong>(
                    "SELECT number FROM numbers(50_000_000)"))
                {
                    rows++;
                }
                return (rows, error: (Exception?)null);
            }
            catch (Exception ex)
            {
                return (rows, error: ex);
            }
        });

        await Task.Delay(300);
        await _fixture.KillContainerAsync();

        var (consumed, error) = await readerTask;
        _output.WriteLine($"Reader consumed {consumed} rows before kill; error: {error?.GetType().FullName}");
        Assert.NotNull(error);
        Assert.IsNotType<OutOfMemoryException>(error);
        Assert.IsNotType<AccessViolationException>(error);

        await _fixture.StartContainerAsync();
        await conn.DisposeAsync();

        await using var fresh = await ds.OpenConnectionAsync();
        Assert.Equal(7, await fresh.ExecuteScalarAsync<int>("SELECT 7"));

        var stats = ds.GetStatistics();
        _output.WriteLine($"Post-recovery stats: {stats}");
        Assert.Equal(0, stats.PendingWaits);
    }

    [Fact]
    public async Task KillContainer_DuringConcurrentBurst_StatsSettleClean()
    {
        await using var ds = BuildDataSource(maxPoolSize: 6);

        // Warm pool.
        var warm = await Task.WhenAll(
            Enumerable.Range(0, 3).Select(_ => ds.OpenConnectionAsync().AsTask()));
        foreach (var c in warm)
        {
            _ = await c.ExecuteScalarAsync<int>("SELECT 1");
            await c.DisposeAsync();
        }

        var cts = new CancellationTokenSource();
        var burst = Task.WhenAll(Enumerable.Range(0, 10).Select(async i =>
        {
            try
            {
                await using var c = await ds.OpenConnectionAsync(cts.Token);
                await c.ExecuteScalarAsync<int>($"SELECT {i}");
            }
            catch { /* expected for in-flight rents at kill time */ }
        }));

        await Task.Delay(120);
        await _fixture.KillContainerAsync();
        await _fixture.StartContainerAsync();

        cts.Cancel();
        await burst;

        await Task.Delay(200);
        var post = ds.GetStatistics();
        _output.WriteLine($"Post-burst stats: {post}");
        Assert.Equal(0, post.Busy);
        Assert.Equal(0, post.PendingWaits);

        await using var final = await ds.OpenConnectionAsync();
        Assert.Equal(99, await final.ExecuteScalarAsync<int>("SELECT 99"));
    }
}
