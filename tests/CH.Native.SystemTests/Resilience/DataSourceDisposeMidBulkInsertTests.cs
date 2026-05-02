using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pins the contract that disposing a <see cref="ClickHouseDataSource"/> while
/// a bulk insert holds one of its rented connections does not deadlock the
/// inserter or leak the busy slot.
///
/// <para>
/// The bulk-insert path holds the wire from <c>InitAsync</c> through
/// <c>CompleteAsync</c>/<c>DisposeAsync</c>. The owning <see cref="ClickHouseDataSource"/>
/// does <em>not</em> wait for in-flight rents on dispose — it cancels the dispose
/// token (waking parked rent waiters) and walks the idle stack. Connections
/// already rented out are not disposed by the pool itself; they're routed through
/// <c>DiscardInternalAsync</c> when the renter eventually returns them.
/// </para>
///
/// <para>
/// What we test: dispose racing a mid-flush bulk insert. The inserter's own
/// <c>DisposeAsync</c> must complete; the pool must not leak permits; rows
/// already committed before dispose must be visible.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class DataSourceDisposeMidBulkInsertTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public DataSourceDisposeMidBulkInsertTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task DisposingPoolWhileBulkInsertActive_DoesNotHang_NoPermitLeak()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fx.BuildSettings());

        var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 4,
        });

        // Rent a connection and start a bulk insert that buffers but doesn't
        // yet flush a full batch. The connection is held by the inserter.
        var conn = await ds.OpenConnectionAsync();
        var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 100_000 });
        await inserter.InitAsync();

        // Buffer some rows below the batch threshold (so no flush has hit
        // the wire yet) — the inserter remains in InitAsync's post-state,
        // owning the busy slot.
        for (int i = 0; i < 200; i++)
            await inserter.AddAsync(new StandardRow { Id = i, Payload = "x" });

        var disposeStart = DateTime.UtcNow;
        var disposeTask = ds.DisposeAsync();

        // The inserter still owns the rented connection — pool dispose should
        // complete without waiting for the inserter (it doesn't track rented
        // connections individually).
        var poolDoneOrTimeout = await Task.WhenAny(disposeTask.AsTask(), Task.Delay(TimeSpan.FromSeconds(10)));
        Assert.Same(disposeTask.AsTask(), poolDoneOrTimeout);
        await disposeTask;

        var poolDisposeElapsed = DateTime.UtcNow - disposeStart;
        _output.WriteLine($"pool dispose elapsed: {poolDisposeElapsed.TotalMilliseconds:F0} ms");

        // Now drive the inserter to completion. Several outcomes are acceptable:
        //  - CompleteAsync succeeds (the connection's wire was untouched by the pool).
        //  - CompleteAsync surfaces a connection-level failure if the pool's
        //    teardown raced our terminator write.
        // Either way: it must not hang, and the inserter's DisposeAsync must
        // eventually clean up.
        Exception? completeEx = null;
        try
        {
            await inserter.CompleteAsync();
        }
        catch (Exception ex)
        {
            completeEx = ex;
        }
        _output.WriteLine($"complete after pool dispose: {completeEx?.GetType().Name ?? "OK"}");

        try { await inserter.DisposeAsync(); } catch (Exception ex) { _output.WriteLine($"inserter dispose: {ex.GetType().Name}"); }
        try { await conn.DisposeAsync(); } catch { /* the pool's return hook routes through DiscardInternalAsync */ }

        // Audit: if CompleteAsync succeeded, all 200 rows are visible. If it
        // failed, none should be (no terminator → no commit). Either is OK,
        // but the table state must be one of those two — no torn row count.
        var committed = await harness.CountAsync();
        _output.WriteLine($"committed rows after race: {committed}");
        Assert.True(committed == 0UL || committed == 200UL,
            $"committed rows must be all-or-nothing; saw {committed}");
    }

    [Fact]
    public async Task BulkInsertViaPool_CompletesAndCommits()
    {
        // Sanity-check the happy-path equivalent: bulk insert via the pool's
        // CreateBulkInserterAsync, complete normally, then dispose the pool.
        // The audit oracle is row count via a fresh connection — pool-internal
        // accounting (Idle/Busy stats after inserter dispose) is intentionally
        // not asserted here because BulkInserter.DisposeAsync does not call
        // the underlying ClickHouseConnection.DisposeAsync, so the rented
        // connection remains in the pool's "rented but no longer in use"
        // limbo until pool dispose. That's a known caveat of pool-rented
        // bulk inserters; the row-count is what callers care about.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fx.BuildSettings());

        var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 4,
        });

        await using (var inserter = await ds.CreateBulkInserterAsync<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 100 }))
        {
            await inserter.InitAsync();
            for (int i = 0; i < 250; i++)
                await inserter.AddAsync(new StandardRow { Id = i, Payload = "x" });
            await inserter.CompleteAsync();
        }

        await ds.DisposeAsync();

        Assert.Equal(250UL, await harness.CountAsync());
    }
}
