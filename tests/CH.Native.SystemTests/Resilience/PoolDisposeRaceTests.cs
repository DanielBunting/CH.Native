using System.Reflection;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pins the pool's dispose-vs-return race (C1). <c>ReturnAsync</c> reads
/// <c>_disposed</c> once, then awaits <c>ResetSessionStateAsync</c> (network I/O
/// when session state is dirty), then pushes to the idle stack. If the DataSource
/// disposes inside that await window, the drained-once idle stack never sees the
/// late push: the connection leaks (socket never disposed) and <c>Total</c> stays
/// inflated. The fix re-checks after the push and drains the stack.
/// </summary>
[Collection("Toxiproxy")]
[Trait(Categories.Name, Categories.Resilience)]
[Trait(Categories.Name, Categories.RaceSensitive)]
public sealed class PoolDisposeRaceTests : IAsyncLifetime
{
    private readonly ToxiproxyFixture _proxy;
    private readonly ITestOutputHelper _output;

    public PoolDisposeRaceTests(ToxiproxyFixture proxy, ITestOutputHelper output)
    {
        _proxy = proxy;
        _output = output;
    }

    public Task InitializeAsync() => _proxy.ResetProxyAsync();
    public Task DisposeAsync() => _proxy.ResetProxyAsync();

    [Fact]
    public async Task DisposeDuringInFlightReturn_ConnectionIsDisposedNotLeaked()
    {
        var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _proxy.BuildSettings(),
            MaxPoolSize = 2,
        });

        // Rent and DIRTY the session so the return path must run a real
        // SET ... = DEFAULT round trip inside ResetSessionStateAsync.
        var conn = await ds.OpenConnectionAsync();
        await conn.ExecuteNonQueryAsync("SET max_threads = 1");

        // Slow the client->server direction so the reset write inside the
        // return path parks mid-flight, holding the return in the race window.
        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "latency", "upstream",
            new() { ["latency"] = 800, ["jitter"] = 0 });

        // Start the return (disposing a pooled connection fires the return hook),
        // then dispose the DataSource while the return is parked in the reset.
        var returning = conn.DisposeAsync();
        await Task.Delay(100); // let the return reach the reset's network write
        var disposing = ds.DisposeAsync();

        await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
        await returning;
        await disposing;

        // The connection must NOT survive as an open socket parked on the dead
        // pool's idle stack.
        var idle = GetIdleCount(ds);
        _output.WriteLine($"post-dispose: conn.IsOpen={conn.IsOpen}, idle entries={idle}");
        Assert.False(conn.IsOpen, "Late-returned connection leaked: still open after pool dispose.");
        Assert.Equal(0, idle);
    }

    private static int GetIdleCount(ClickHouseDataSource ds)
    {
        var field = typeof(ClickHouseDataSource).GetField("_idle", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var stack = (System.Collections.ICollection)field.GetValue(ds)!;
        return stack.Count;
    }
}
