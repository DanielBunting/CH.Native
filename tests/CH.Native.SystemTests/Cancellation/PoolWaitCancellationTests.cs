using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Cancellation;

/// <summary>
/// Pool-saturation cancellation contract: cancelling a queued <c>OpenConnectionAsync</c>
/// must propagate quickly, decrement <c>PendingWaits</c>, and not strand the queue slot.
/// Existing cancellation tests cover mid-roundtrip cancel; this is the orthogonal case
/// where the caller never makes it into the wire at all.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Cancellation)]
public class PoolWaitCancellationTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public PoolWaitCancellationTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private ClickHouseDataSource BuildDataSource(int maxPoolSize, TimeSpan? waitTimeout = null) =>
        new(new ClickHouseDataSourceOptions
        {
            Settings = _fixture.BuildSettings(),
            MaxPoolSize = maxPoolSize,
            ConnectionWaitTimeout = waitTimeout ?? TimeSpan.FromSeconds(30),
        });

    [Fact]
    public async Task CancelWhileQueuedForPoolSlot_ThrowsPromptly()
    {
        await using var ds = BuildDataSource(maxPoolSize: 2);

        // Saturate the pool: two connections each running a long sleep query.
        var rented1 = await ds.OpenConnectionAsync();
        var rented2 = await ds.OpenConnectionAsync();

        var busy1 = Task.Run(() => rented1.ExecuteScalarAsync<int>("SELECT sleep(10)"));
        var busy2 = Task.Run(() => rented2.ExecuteScalarAsync<int>("SELECT sleep(10)"));

        try
        {
            // Third rent must queue: stats should report PendingWaits == 1 within
            // a brief window. Poll because the rent path increments before parking.
            using var cts = new CancellationTokenSource();
            var queuedRent = ds.OpenConnectionAsync(cts.Token).AsTask();

            var deadline = DateTime.UtcNow.AddSeconds(2);
            int observedPending = 0;
            while (DateTime.UtcNow < deadline)
            {
                observedPending = ds.GetStatistics().PendingWaits;
                if (observedPending >= 1) break;
                await Task.Delay(20);
            }
            _output.WriteLine($"PendingWaits observed: {observedPending}");
            Assert.True(observedPending >= 1, $"Third rent should queue (PendingWaits ≥ 1); saw {observedPending}.");

            // Cancel and time how long it takes for OperationCanceledException to surface.
            var sw = Stopwatch.StartNew();
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queuedRent);
            sw.Stop();
            _output.WriteLine($"Cancel-to-throw: {sw.ElapsedMilliseconds}ms");
            Assert.True(sw.ElapsedMilliseconds < 500,
                $"Cancel of queued rent should propagate fast; took {sw.ElapsedMilliseconds}ms.");

            // Pending waits must drop back to 0 — a leak here strands future capacity.
            Assert.Equal(0, ds.GetStatistics().PendingWaits);
        }
        finally
        {
            // Release the saturating rents so the pool tears down cleanly.
            await rented1.DisposeAsync();
            await rented2.DisposeAsync();
            try { await busy1; } catch { /* sleep was cancelled by connection dispose */ }
            try { await busy2; } catch { }
        }
    }

    [Fact]
    public async Task AfterQueuedCancel_FreshOpenSucceeds()
    {
        await using var ds = BuildDataSource(maxPoolSize: 2, waitTimeout: TimeSpan.FromSeconds(10));

        var rented1 = await ds.OpenConnectionAsync();
        var rented2 = await ds.OpenConnectionAsync();

        var busy1 = Task.Run(() => rented1.ExecuteScalarAsync<int>("SELECT sleep(5)"));
        var busy2 = Task.Run(() => rented2.ExecuteScalarAsync<int>("SELECT sleep(5)"));

        try
        {
            using var cts = new CancellationTokenSource();
            var queuedRent = ds.OpenConnectionAsync(cts.Token).AsTask();
            await Task.Delay(150);
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queuedRent);

            // Free one slot by disposing one of the saturating rents.
            await rented1.DisposeAsync();
            try { await busy1; } catch { /* server cancelled the sleep */ }

            // A fresh open must succeed within the freed-slot timing — proves no
            // queue entry was leaked from the cancelled call.
            var sw = Stopwatch.StartNew();
            await using var fresh = await ds.OpenConnectionAsync();
            sw.Stop();
            _output.WriteLine($"Fresh-rent latency after cancelled queue: {sw.ElapsedMilliseconds}ms");
            Assert.True(sw.ElapsedMilliseconds < 1000,
                $"Fresh rent after cancel should be fast; took {sw.ElapsedMilliseconds}ms.");

            var v = await fresh.ExecuteScalarAsync<int>("SELECT 1");
            Assert.Equal(1, v);
        }
        finally
        {
            await rented2.DisposeAsync();
            try { await busy2; } catch { }
        }
    }

    [Fact]
    public async Task MultipleQueuedCancellations_AllReleased()
    {
        await using var ds = BuildDataSource(maxPoolSize: 2, waitTimeout: TimeSpan.FromSeconds(10));

        var rented1 = await ds.OpenConnectionAsync();
        var rented2 = await ds.OpenConnectionAsync();
        var busy1 = Task.Run(() => rented1.ExecuteScalarAsync<int>("SELECT sleep(5)"));
        var busy2 = Task.Run(() => rented2.ExecuteScalarAsync<int>("SELECT sleep(5)"));

        const int queueDepth = 5;
        var ctsList = Enumerable.Range(0, queueDepth).Select(_ => new CancellationTokenSource()).ToArray();
        try
        {
            var rents = ctsList.Select(cts => ds.OpenConnectionAsync(cts.Token).AsTask()).ToArray();

            // Wait until all five are parked on the gate.
            var deadline = DateTime.UtcNow.AddSeconds(2);
            while (DateTime.UtcNow < deadline && ds.GetStatistics().PendingWaits < queueDepth)
                await Task.Delay(20);
            Assert.True(ds.GetStatistics().PendingWaits >= queueDepth,
                $"Expected {queueDepth} parked waiters; saw {ds.GetStatistics().PendingWaits}.");

            // Cancel all five; each must throw and the queue must drain.
            foreach (var c in ctsList) c.Cancel();
            foreach (var r in rents)
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => r);

            Assert.Equal(0, ds.GetStatistics().PendingWaits);

            // Sixth open — after a slot frees up — succeeds, proving no leaked queue entries.
            await rented1.DisposeAsync();
            try { await busy1; } catch { }

            await using var fresh = await ds.OpenConnectionAsync();
            Assert.Equal(1, await fresh.ExecuteScalarAsync<int>("SELECT 1"));
        }
        finally
        {
            foreach (var c in ctsList) c.Dispose();
            await rented2.DisposeAsync();
            try { await busy2; } catch { }
        }
    }
}
