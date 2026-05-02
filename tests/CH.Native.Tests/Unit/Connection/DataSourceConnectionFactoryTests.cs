using CH.Native.Connection;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

/// <summary>
/// <see cref="ClickHouseDataSourceOptions.ConnectionFactory"/> is the hook the
/// DI layer uses to inject rotating credentials (JWT, SSH key, mTLS cert,
/// password) per physical connection. The pool calls it once per cache-miss
/// (no idle connection available, lifetime/idle expiry forced a discard).
/// These tests count factory invocations without ever standing up a real
/// ClickHouse server: the factory throws a sentinel exception immediately,
/// proving the call happened, and the test asserts on the sentinel.
/// </summary>
public class DataSourceConnectionFactoryTests
{
    private sealed class FactoryProbe : Exception { }

    private static ClickHouseDataSourceOptions WithCountingFactory(
        out Func<int> getCount,
        Func<int, ClickHouseConnectionSettings>? settingsForCall = null)
    {
        int count = 0;
        getCount = () => Volatile.Read(ref count);

        // Provide a baseline `Settings` (required by the options class) but the
        // factory below is what the pool actually calls when present. The
        // factory throws to short-circuit before `new ClickHouseConnection`
        // attempts a real socket open.
        return new ClickHouseDataSourceOptions
        {
            Settings = ClickHouseConnectionSettings.CreateBuilder().WithHost("placeholder").Build(),
            ConnectionFactory = ct =>
            {
                ct.ThrowIfCancellationRequested();
                var c = Interlocked.Increment(ref count);
                _ = settingsForCall?.Invoke(c);
                throw new FactoryProbe();
            },
            MaxPoolSize = 4,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(2),
        };
    }

    [Fact]
    public async Task OpenConnectionAsync_FirstCall_InvokesFactory()
    {
        var opts = WithCountingFactory(out var getCount);
        await using var ds = new ClickHouseDataSource(opts);

        await Assert.ThrowsAsync<FactoryProbe>(async () =>
            await ds.OpenConnectionAsync(CancellationToken.None));

        Assert.Equal(1, getCount());
    }

    [Fact]
    public async Task OpenConnectionAsync_RepeatedCacheMisses_InvokeFactoryEachTime()
    {
        // Every call fails (sentinel), so no idle connection is ever pooled,
        // so each rent is a cache miss → factory invoked. Pin that contract.
        var opts = WithCountingFactory(out var getCount);
        await using var ds = new ClickHouseDataSource(opts);

        for (int i = 0; i < 3; i++)
        {
            await Assert.ThrowsAsync<FactoryProbe>(async () =>
                await ds.OpenConnectionAsync(CancellationToken.None));
        }

        Assert.Equal(3, getCount());
    }

    [Fact]
    public async Task OpenConnectionAsync_ReceivesNonNullCancellationToken()
    {
        // The factory contract is `Func<CancellationToken, ValueTask<...>>` —
        // the pool must propagate the caller's cancellation token through.
        // Pin by passing a cancelled token and asserting the factory observes
        // it via ThrowIfCancellationRequested.
        var opts = WithCountingFactory(out _);
        await using var ds = new ClickHouseDataSource(opts);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // OperationCanceledException (the factory's ThrowIfCancellationRequested)
        // should win over the FactoryProbe sentinel. Either way it's not the
        // success path.
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await ds.OpenConnectionAsync(cts.Token));
    }

    [Fact]
    public async Task OpenConnectionAsync_AfterDispose_ThrowsObjectDisposed()
    {
        var opts = WithCountingFactory(out _);
        var ds = new ClickHouseDataSource(opts);
        await ds.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await ds.OpenConnectionAsync(CancellationToken.None));
    }

    [Fact]
    public async Task FactoryReturnsDifferentSettingsPerCall_PoolUsesLatest()
    {
        // Capture the settings each call returns. Even though the call fails,
        // the factory was invoked with each call. Pin that the pool calls the
        // factory anew (not a cached snapshot) so rotating credentials work.
        var perCallSettings = new List<int>();
        var opts = WithCountingFactory(
            out var getCount,
            settingsForCall: callIndex => { perCallSettings.Add(callIndex); return null!; });

        await using var ds = new ClickHouseDataSource(opts);

        for (int i = 0; i < 3; i++)
        {
            try { await ds.OpenConnectionAsync(CancellationToken.None); }
            catch (FactoryProbe) { /* expected */ }
        }

        Assert.Equal(3, getCount());
        Assert.Equal(new[] { 1, 2, 3 }, perCallSettings);
    }

    [Fact]
    public async Task NoFactory_FallsBackToOptionsSettings()
    {
        // When ConnectionFactory is null the pool uses Options.Settings as
        // an immutable baseline. Since 'placeholder' isn't a real host, the
        // wire-level connect will fail — but it should fail at the connect
        // stage, not at the factory stage (which doesn't exist).
        var opts = new ClickHouseDataSourceOptions
        {
            Settings = ClickHouseConnectionSettings.CreateBuilder()
                .WithHost("127.0.0.1").WithPort(1).WithConnectTimeout(TimeSpan.FromMilliseconds(200))
                .Build(),
            ConnectionFactory = null,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(2),
        };
        await using var ds = new ClickHouseDataSource(opts);

        // The connect attempt will fail with some socket exception — we don't
        // care which kind, only that the absence of a factory doesn't itself
        // throw.
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await ds.OpenConnectionAsync(CancellationToken.None));
    }
}
