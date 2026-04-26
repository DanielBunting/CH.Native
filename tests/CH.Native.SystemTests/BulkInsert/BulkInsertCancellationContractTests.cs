using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Extends <c>BulkInsertCancelPoisoningTests</c> with state-inspector assertions on
/// the post-cancel inserter. Pins the contrast between cancellation (clears the
/// buffer, sets <c>_completeStarted</c>) and a network failure (preserves the
/// buffer; see <see cref="BulkInsertBufferSurvivalTests"/>) — both behaviours are
/// load-bearing and must not converge by accident.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Cancellation)]
public class BulkInsertCancellationContractTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public BulkInsertCancellationContractTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task Cancellation_DuringInsert_ClearsBuffer_AndSetsCompleteStarted()
    {
        // Cancel while the inserter is mid-stream. ObserveCancellationAsync runs
        // the slow path: SendCancel → Abort → Drain → throw. Abort clears the
        // buffer and flips _completeStarted. Both flags are observable; pin them.
        //
        // Determinism note: a wall-clock cancellation token would race a fast
        // localhost insert (the workload can complete in <50 ms). To pin the
        // post-cancel state reliably we initialise normally, buffer some rows,
        // then explicitly cancel before CompleteAsync — guaranteeing the
        // cancellation slow path runs.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 1000 });
        await inserter.InitAsync();

        var s = new string('z', 256);
        for (int i = 0; i < 100; i++)
            await inserter.AddAsync(new StandardRow { Id = i, Payload = s });

        using (var cts = new CancellationTokenSource())
        {
            cts.Cancel();
            try { await inserter.CompleteAsync(cts.Token); }
            catch (OperationCanceledException) { /* expected */ }
            catch (Exception ex)
            {
                _output.WriteLine($"BulkInsert cancellation surfaced as: {ex.GetType().Name}: {ex.Message}");
            }
        }

        // The cancellation drain (Abort) clears the buffer and sets
        // _completeStarted. Pin both: they signal Dispose not to retry.
        Assert.Equal(0, inserter.BufferedCount);
        Assert.True(InserterStateInspector.CompleteStarted(inserter),
            "_completeStarted must be set by the cancellation Abort path so Dispose does not retry.");
        Assert.False(InserterStateInspector.Completed(inserter),
            "_completed remains false because the INSERT was cancelled, not committed.");

        try { await inserter.DisposeAsync(); } catch { /* tolerate */ }

        // Connection still usable — the existing poisoning test asserts this in
        // the integration sense; we re-assert it under the state-inspector
        // contract.
        Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
    }

    [Fact]
    public async Task CompleteAsync_AfterCancellation_SecondCall_DoesNotPoisonConnection()
    {
        // BUG-EXPOSING TEST. Currently FAILS against main.
        //
        // After a cancelled CompleteAsync, _completeStarted=true and
        // _completed=false. The guard at BulkInserter.cs:494 only short-
        // circuits on _completed, so a second call re-runs FlushAsync +
        // SendEmptyBlock + ReceiveEndOfStream against a server that already
        // moved on after the cancel. The server replies "Unexpected packet
        // Data received from client" and closes the wire, poisoning the
        // connection.
        //
        // The fix is a one-line guard at BulkInserter.cs:494
        // ("if (_completeStarted) return;" or "throw"). Once that ships,
        // this test will pass — the second CompleteAsync becomes a no-op
        // (or a deterministic throw without server contact), and the
        // connection remains usable.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 1000 });
        await inserter.InitAsync();
        for (int i = 0; i < 10; i++)
            await inserter.AddAsync(new StandardRow { Id = i, Payload = "y" });

        using (var cts = new CancellationTokenSource())
        {
            cts.Cancel();
            try { await inserter.CompleteAsync(cts.Token); }
            catch (OperationCanceledException) { /* expected */ }
        }

        Assert.True(InserterStateInspector.CompleteStarted(inserter));

        // Second CompleteAsync — must not hang. Bound it with a timeout so a
        // hang surfaces as a TimeoutException rather than a stalled test.
        var second = inserter.CompleteAsync();
        var winner = await Task.WhenAny(second, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.Same(second, winner);

        // The second call may legitimately throw (idempotent-throw is fine), or
        // it may no-op. What it MUST NOT do is touch the wire in a way that
        // confuses the server.
        try { await second; }
        catch (Exception ex)
        {
            _output.WriteLine($"Second CompleteAsync threw: {ex.GetType().Name}: {ex.Message}");
        }

        try { await inserter.DisposeAsync(); } catch { /* tolerate */ }

        // The load-bearing assertion: after the cancelled-then-retried
        // CompleteAsync sequence, the underlying connection MUST still work.
        // Today this fails — server closed the wire because the second
        // CompleteAsync sent unexpected Data. Once the guard is added, this
        // will pass.
        var probe = await conn.ExecuteScalarAsync<int>("SELECT 1");
        Assert.Equal(1, probe);
    }

    [Fact]
    public async Task AddRangeAsync_AfterCancelledComplete_RejectsRows()
    {
        // Same root cause as AddAsync — _completeStarted=true post-cancel must
        // reject the next row regardless of which Add* overload the caller used.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 1000 });
        await inserter.InitAsync();
        await inserter.AddAsync(new StandardRow { Id = 0, Payload = "first" });

        using (var cts = new CancellationTokenSource())
        {
            cts.Cancel();
            try { await inserter.CompleteAsync(cts.Token); }
            catch (OperationCanceledException) { /* expected */ }
        }

        Assert.True(InserterStateInspector.CompleteStarted(inserter));

        await Assert.ThrowsAnyAsync<InvalidOperationException>(async () =>
        {
            await inserter.AddRangeAsync(new[]
            {
                new StandardRow { Id = 1, Payload = "after-cancel-1" },
                new StandardRow { Id = 2, Payload = "after-cancel-2" },
            });
        });

        try { await inserter.DisposeAsync(); } catch { /* tolerate */ }
    }

    [Fact]
    public async Task AddRangeStreamingAsync_Enumerable_AfterCancelledComplete_RejectsRows()
    {
        // Same guard as AddAsync, applied to the IEnumerable streaming overload.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 1000 });
        await inserter.InitAsync();
        await inserter.AddAsync(new StandardRow { Id = 0, Payload = "first" });

        using (var cts = new CancellationTokenSource())
        {
            cts.Cancel();
            try { await inserter.CompleteAsync(cts.Token); }
            catch (OperationCanceledException) { /* expected */ }
        }

        Assert.True(InserterStateInspector.CompleteStarted(inserter));

        await Assert.ThrowsAnyAsync<InvalidOperationException>(async () =>
        {
            await inserter.AddRangeStreamingAsync(new[]
            {
                new StandardRow { Id = 1, Payload = "x" },
            });
        });

        try { await inserter.DisposeAsync(); } catch { /* tolerate */ }
    }

    [Fact]
    public async Task AddRangeStreamingAsync_AsyncEnumerable_AfterCancelledComplete_RejectsRows()
    {
        // Same guard as AddAsync, applied to the IAsyncEnumerable streaming overload.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 1000 });
        await inserter.InitAsync();
        await inserter.AddAsync(new StandardRow { Id = 0, Payload = "first" });

        using (var cts = new CancellationTokenSource())
        {
            cts.Cancel();
            try { await inserter.CompleteAsync(cts.Token); }
            catch (OperationCanceledException) { /* expected */ }
        }

        Assert.True(InserterStateInspector.CompleteStarted(inserter));

        await Assert.ThrowsAnyAsync<InvalidOperationException>(async () =>
        {
            await inserter.AddRangeStreamingAsync(AsyncRows());
        });

        try { await inserter.DisposeAsync(); } catch { /* tolerate */ }

        static async IAsyncEnumerable<StandardRow> AsyncRows()
        {
            await Task.Yield();
            yield return new StandardRow { Id = 1, Payload = "x" };
        }
    }

    [Fact]
    public async Task FlushAsync_AfterCancelledComplete_NoOpsWithoutPoisoningConnection()
    {
        // The AddAsync guard prevents new rows from landing in the buffer
        // post-cancel, so the buffer is provably empty here and FlushAsync
        // no-ops naturally without sending any wire data. Pin that contract:
        // calling FlushAsync after a cancelled complete must NOT touch the
        // wire (no Data packet that would re-trigger the same Bug-2 server
        // poisoning), and the underlying connection must remain usable.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 1000 });
        await inserter.InitAsync();
        await inserter.AddAsync(new StandardRow { Id = 0, Payload = "first" });

        using (var cts = new CancellationTokenSource())
        {
            cts.Cancel();
            try { await inserter.CompleteAsync(cts.Token); }
            catch (OperationCanceledException) { /* expected */ }
        }

        Assert.True(InserterStateInspector.CompleteStarted(inserter));
        Assert.Equal(0, inserter.BufferedCount);

        // Should not throw, should not poison the wire — the empty-buffer
        // short-circuit returns before any I/O.
        await inserter.FlushAsync();

        try { await inserter.DisposeAsync(); } catch { /* tolerate */ }

        var probe = await conn.ExecuteScalarAsync<int>("SELECT 1");
        Assert.Equal(1, probe);
    }

    [Fact]
    public async Task CompleteAsync_DoubleCall_AfterSuccess_IsNoOp()
    {
        // The new guard at CompleteAsync (`if (_completed || _completeStarted) return;`)
        // applies to both the cancelled path (covered by
        // CompleteAsync_AfterCancellation_SecondCall_DoesNotPoisonConnection)
        // and the happy path. Pin the happy-path idempotency: a second
        // CompleteAsync after a successful one must no-op without touching
        // the wire and the connection must remain usable.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 100 });
        await inserter.InitAsync();
        for (int i = 0; i < 10; i++)
            await inserter.AddAsync(new StandardRow { Id = i, Payload = "row" });
        await inserter.CompleteAsync();

        Assert.True(InserterStateInspector.Completed(inserter));

        // Second call must return without contacting the server.
        await inserter.CompleteAsync();

        try { await inserter.DisposeAsync(); } catch { /* expected: clean dispose */ }

        Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
        Assert.Equal(10UL, await harness.CountAsync());
    }

    [Fact]
    public async Task AddAsync_AfterCancelledComplete_RejectsRow()
    {
        // BUG-EXPOSING TEST. Currently FAILS against main.
        //
        // After a cancelled CompleteAsync, _completeStarted=true. The
        // CompleteAsync drain has already aborted the wire's INSERT state, so
        // any further rows added to the buffer cannot land. AddAsync's guard
        // at BulkInserter.cs:215 only checks _completed (which is false here),
        // so the call silently succeeds, buffering rows that will never be
        // sent — silent data loss the moment the inserter is disposed.
        //
        // The fix is a one-line guard in AddAsync (and AddRangeAsync): also
        // reject when _completeStarted is true. With that guard, this test
        // passes.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 1000 });
        await inserter.InitAsync();
        await inserter.AddAsync(new StandardRow { Id = 0, Payload = "first" });

        using (var cts = new CancellationTokenSource())
        {
            cts.Cancel();
            try { await inserter.CompleteAsync(cts.Token); }
            catch (OperationCanceledException) { /* expected */ }
        }

        Assert.True(InserterStateInspector.CompleteStarted(inserter));

        // The load-bearing assertion: AddAsync on an inserter whose Complete
        // was cancelled MUST reject. Anything else is silent data loss.
        await Assert.ThrowsAnyAsync<InvalidOperationException>(async () =>
        {
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = "after-cancel" });
        });

        try { await inserter.DisposeAsync(); } catch { /* tolerate */ }
    }
}
