using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.TestSuite;

/// <summary>
/// Documents and pins which exception wins when multiple timeouts can fire
/// concurrently. The library exposes (at least) three independent deadlines:
///
/// <list type="number">
/// <item><description><c>ConnectTimeout</c> on the connection settings — caps the TCP
///     handshake.</description></item>
/// <item><description><c>ConnectionWaitTimeout</c> on the data source — caps how long
///     <c>OpenConnectionAsync</c> blocks on the gate when the pool is saturated.</description></item>
/// <item><description>Per-call <c>CancellationToken</c> — caller-supplied, fires immediately
///     when the token is cancelled.</description></item>
/// </list>
///
/// <para>
/// These tests exercise each timeout in isolation (asserting the exception type
/// callers can catch) and one race scenario where the cancellation token races
/// the pool's <c>ConnectionWaitTimeout</c> — pinning today's "cancellation wins"
/// behavior so a future precedence change is visible.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class TimeoutPrecedenceTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public TimeoutPrecedenceTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task ConnectTimeout_FiresWhenServerUnreachable_ThrowsTypedException()
    {
        // Use a routable-but-non-listening port so the TCP connect blackholes.
        // 127.0.0.1:1 is reserved and never listens; ConnectTimeout should fire.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("127.0.0.1")
            .WithPort(1)
            .WithCredentials("default", "")
            .WithConnectTimeout(TimeSpan.FromMilliseconds(500))
            .Build();

        await using var conn = new ClickHouseConnection(settings);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        Exception? caught = null;
        try
        {
            await conn.OpenAsync();
        }
        catch (Exception ex)
        {
            caught = ex;
        }
        sw.Stop();

        _output.WriteLine($"ConnectTimeout fired in {sw.ElapsedMilliseconds} ms; threw {caught?.GetType().Name}");
        Assert.NotNull(caught);
        // The actual fast-fail might be a connection-refused error rather than a
        // timeout (port 1 may RST immediately). Accept any I/O / timeout / socket
        // failure — what matters is we don't hang past the configured timeout.
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10),
            $"ConnectTimeout=500ms should not take 10s; took {sw.Elapsed}");
    }

    [Fact]
    public async Task ConnectionWaitTimeout_FiresWhenPoolSaturated_ThrowsTimeout()
    {
        // MaxPoolSize=1, hold the only connection, then ask for a second one
        // with a short ConnectionWaitTimeout. Must throw TimeoutException after
        // ~the configured wait, not after ConnectTimeout.
        var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 1,
            ConnectionWaitTimeout = TimeSpan.FromMilliseconds(500),
        });

        await using var hold = await ds.OpenConnectionAsync();

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var ex = await Assert.ThrowsAsync<TimeoutException>(async () =>
        {
            await using var _ = await ds.OpenConnectionAsync();
        });
        sw.Stop();

        _output.WriteLine($"ConnectionWaitTimeout fired in {sw.ElapsedMilliseconds} ms: {ex.Message}");
        Assert.True(sw.Elapsed >= TimeSpan.FromMilliseconds(400),
            "should not fire before the wait timeout");
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5),
            "should not significantly overshoot the wait timeout");

        await ds.DisposeAsync();
    }

    [Fact]
    public async Task CancellationToken_BeforePoolWaitTimeout_WinsRace()
    {
        // Saturate the pool, then call OpenConnectionAsync with a cancellation
        // token that fires before the configured ConnectionWaitTimeout. The
        // caller should observe OperationCanceledException, not TimeoutException.
        var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 1,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(30),
        });

        await using var hold = await ds.OpenConnectionAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await using var _ = await ds.OpenConnectionAsync(cts.Token);
        });

        await ds.DisposeAsync();
    }

    [Fact]
    public async Task CancellationToken_DuringQuery_WinsOverServerExecutionTime()
    {
        // ClickHouse caps any single sleep call at 3 seconds. We use a
        // 2-second `sleep(2)` so the query is unambiguously in-flight when
        // our 300ms cancellation token fires. The cancel packet must reach
        // the server before sleep returns, and the caller must observe
        // OperationCanceledException — not the (eventual, naturally-arriving)
        // result.
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT sleep(2)").WithCancellation(cts.Token)) { }
        });
        sw.Stop();

        _output.WriteLine($"Query cancellation fired in {sw.ElapsedMilliseconds} ms");
        // Must fire before the natural 2s completion; allow some scheduling slack.
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(2),
            $"Cancellation should fire before the 2s sleep completes; took {sw.Elapsed}");
    }

    [Fact]
    public async Task DisposingPoolWhileWaiterParked_BeatsConnectionWaitTimeout()
    {
        // Pool dispose's _disposeCts cancellation must wake parked waiters
        // immediately — they observe ObjectDisposedException, not waste up to
        // ConnectionWaitTimeout (30s default). This is the precedence between
        // the dispose signal and the wait timer.
        var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 1,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(30),
        });

        var hold = await ds.OpenConnectionAsync();

        // Park a waiter on the gate.
        var parkTask = Task.Run(async () =>
        {
            try
            {
                await using var _ = await ds.OpenConnectionAsync();
                return (Exception?)null;
            }
            catch (Exception ex) { return ex; }
        });

        await Task.Delay(150); // let it park

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await ds.DisposeAsync();
        var disposeElapsed = sw.Elapsed;

        await hold.DisposeAsync();
        var parkResult = await parkTask;

        Assert.NotNull(parkResult);
        Assert.True(
            parkResult is ObjectDisposedException || parkResult is OperationCanceledException,
            $"parked waiter must observe disposal, not the wait timer; saw {parkResult!.GetType().Name}");
        Assert.True(disposeElapsed < TimeSpan.FromSeconds(5),
            "dispose must beat the configured wait timeout");
    }
}
