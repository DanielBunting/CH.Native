using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Disposal-time edge cases: implicit-complete on an empty inserter, dispose after
/// a failed CompleteAsync, and concurrent dispose during an in-flight FlushAsync.
/// Pins the contract documented in <c>BulkInserter.DisposeAsync</c> XML docs:
/// "Dispose is teardown, not commit. Already-attempted complete is not retried."
/// </summary>
[Collection("Toxiproxy")]
[Trait(Categories.Name, Categories.Chaos)]
public class BulkInsertDisposalRaceTests : IAsyncLifetime
{
    private readonly ToxiproxyFixture _proxy;
    private readonly ITestOutputHelper _output;

    public BulkInsertDisposalRaceTests(ToxiproxyFixture proxy, ITestOutputHelper output)
    {
        _proxy = proxy;
        _output = output;
    }

    public Task InitializeAsync() => _proxy.ResetProxyAsync();
    public Task DisposeAsync() => _proxy.ResetProxyAsync();

    [Fact]
    public async Task Dispose_WithoutComplete_ZeroRowsBuffered_ImplicitComplete_PersistsZeroRows()
    {
        // BulkInserter.DisposeAsync (line ~608) calls CompleteAsync implicitly
        // when no rows are buffered, finalising the wire so the connection is
        // reusable. Pin: no throw, count() == 0.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _proxy.BuildSettings());

        await using (var conn = new ClickHouseConnection(_proxy.BuildSettings()))
        {
            await conn.OpenAsync();
            var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName);
            await inserter.InitAsync();
            // No AddAsync; expect implicit complete on dispose.
            await inserter.DisposeAsync();
            Assert.True(InserterStateInspector.Disposed(inserter));

            // Connection still usable.
            Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
        }

        Assert.Equal(0UL, await harness.CountAsync());
    }

    [Fact]
    public async Task Dispose_AfterCompleteThrew_DoesNotRetryWire()
    {
        // CompleteAsync sets _completeStarted = true before sending the
        // terminator; if Receive fails, the catch re-throws. Dispose must NOT
        // retry CompleteAsync (which would hit a poisoned wire and mask the
        // original failure). Pin via the _completeStarted flag.
        const int batchSize = 100;

        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _proxy.BuildSettings());

        await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
        await conn.OpenAsync();
        var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = batchSize });
        await inserter.InitAsync();

        // Add a small batch so CompleteAsync has work to do.
        for (int i = 0; i < 10; i++)
            await inserter.AddAsync(new StandardRow { Id = i, Payload = "x" });

        // Sever the link before CompleteAsync runs the terminator + receive.
        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });

        Exception? completeFailure = null;
        try
        {
            await inserter.CompleteAsync();
        }
        catch (Exception ex)
        {
            completeFailure = ex;
        }

        Assert.NotNull(completeFailure);
        _output.WriteLine($"CompleteAsync failed (expected): {completeFailure!.GetType().Name}");

        // Pin the post-failure state: _completeStarted is set even though Complete
        // never returned successfully. This is what tells DisposeAsync not to retry.
        Assert.True(InserterStateInspector.CompleteStarted(inserter),
            "_completeStarted must be set before the wire-terminator attempt so Dispose does not retry.");
        Assert.False(InserterStateInspector.Completed(inserter),
            "_completed must remain false because the receive never confirmed success.");

        // Dispose must not throw a *second* failure — the contract is "no retry."
        await inserter.DisposeAsync();
        Assert.True(InserterStateInspector.Disposed(inserter));
    }

    [Fact]
    public async Task Dispose_DuringInFlightFlush_OneTaskWins_NoCorruption()
    {
        // FlushAsync and DisposeAsync running concurrently. The contract is
        // permissive: one of the two surfaces an error or both succeed, but
        // afterwards a *fresh* connection's SELECT 1 must return 1. The wire
        // state of the doomed connection is not asserted — only that the server
        // is not stuck and a new connection is healthy.
        const int batchSize = 1000;

        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _proxy.BuildSettings());

        // Throttle so the flush takes long enough for the dispose race to be real.
        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "bandwidth", "upstream",
            new() { ["rate"] = 64 });

        await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
        await conn.OpenAsync();
        var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = batchSize });
        await inserter.InitAsync();

        var s = new string('y', 1024);
        for (int i = 0; i < batchSize - 1; i++)
            await inserter.AddAsync(new StandardRow { Id = i, Payload = s });

        // Kick off Flush + Dispose concurrently. Race outcomes vary; we only
        // assert no protocol corruption escapes.
        var flushTask = Task.Run(async () =>
        {
            try
            {
                await inserter.AddAsync(new StandardRow { Id = batchSize - 1, Payload = s });
                await inserter.FlushAsync();
                return (Outcome: "flush-ok", Error: (Exception?)null);
            }
            catch (Exception ex) { return (Outcome: "flush-error", Error: ex); }
        });

        var disposeTask = Task.Run(async () =>
        {
            // Tiny stagger so the flush has a chance to start.
            await Task.Delay(10);
            try
            {
                await inserter.DisposeAsync();
                return (Outcome: "dispose-ok", Error: (Exception?)null);
            }
            catch (Exception ex) { return (Outcome: "dispose-error", Error: ex); }
        });

        var results = await Task.WhenAll(flushTask, disposeTask);
        foreach (var r in results)
            _output.WriteLine($"{r.Outcome}: {r.Error?.GetType().Name ?? "no error"}");

        // Remove toxic so the audit and probe are not also throttled.
        await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);

        // Server is not stuck — fresh connection works.
        await using var fresh = new ClickHouseConnection(_proxy.BuildSettings());
        await fresh.OpenAsync();
        Assert.Equal(1, await fresh.ExecuteScalarAsync<int>("SELECT 1"));
    }
}
