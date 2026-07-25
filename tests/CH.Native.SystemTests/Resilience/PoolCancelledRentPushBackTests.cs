using System.Reflection;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Repro for wire-state-machine review finding #3: the cancelled-rent-during-ping
/// path does <c>_idle.Push(entry)</c> with no post-push <c>_disposed</c> re-check —
/// the exact race <c>ReturnAsync</c> closes with its re-check-and-drain — and no
/// health check. If the DataSource disposes while the validate-on-rent ping is in
/// flight, the push lands on the already-drained idle stack: the connection is
/// stranded as a live socket nobody ever disposes and <c>_total</c> stays inflated.
/// </summary>
[Collection("Toxiproxy")]
[Trait(Categories.Name, Categories.Resilience)]
[Trait(Categories.Name, Categories.RaceSensitive)]
public sealed class PoolCancelledRentPushBackTests : IAsyncLifetime
{
    private readonly ToxiproxyFixture _proxy;
    private readonly ITestOutputHelper _output;

    public PoolCancelledRentPushBackTests(ToxiproxyFixture proxy, ITestOutputHelper output)
    {
        _proxy = proxy;
        _output = output;
    }

    public Task InitializeAsync() => _proxy.ResetProxyAsync();
    public Task DisposeAsync() => _proxy.ResetProxyAsync();

    [Fact]
    public async Task RentCancelledDuringPing_AfterPoolDispose_DoesNotStrandConnection()
    {
        var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _proxy.BuildSettings(),
            MaxPoolSize = 2,
            ValidateOnRent = true,
        });

        // Seed exactly one idle connection.
        var seed = await ds.OpenConnectionAsync();
        await seed.DisposeAsync();
        Assert.Equal(1, GetIdleCount(ds));

        // Block server->client so the validate-on-rent ping parks awaiting its
        // SELECT 1 response.
        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "timeout", "downstream",
            new() { ["timeout"] = 0 });

        using var cts = new CancellationTokenSource();
        var rentTask = ds.OpenConnectionAsync(cts.Token).AsTask();
        await Task.Delay(500); // let the rent pop the idle entry and enter the ping

        // Dispose the pool while the ping holds the popped entry: DisposeAsyncCore
        // drains _idle exactly once and the stack is empty at this moment.
        await ds.DisposeAsync();

        // Cancel the rent, then unblock the wire so the ping's post-OCE drain can
        // consume the parked response and the rent reaches its push-back path.
        cts.Cancel();
        await Task.Delay(200);
        await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => rentTask);

        // The popped entry must not be stranded on the DISPOSED pool's idle
        // stack as a live socket — it has to be discarded like every other
        // late-return path (ReturnAsync's post-push re-check).
        var idle = GetIdleCount(ds);
        _output.WriteLine($"idle entries on disposed pool: {idle}");
        Assert.Equal(0, idle);
    }

    private static int GetIdleCount(ClickHouseDataSource ds)
    {
        var field = typeof(ClickHouseDataSource).GetField("_idle", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var stack = (System.Collections.ICollection)field.GetValue(ds)!;
        return stack.Count;
    }
}
