using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pins the buffer-survival contract observed in <c>BulkInserter.FlushAsync</c>:
/// a non-cancellation flush failure leaves the buffer untouched so the caller can
/// retry on a fresh connection. The corollary tests cover what CompleteAsync does
/// when the wire is severed mid-insert.
/// </summary>
/// <remarks>
/// Pure network chaos is a poor fit for the FlushAsync-exception path. The
/// inserter's <c>SendDataBlockAsync</c> writes to a <c>System.IO.Pipelines</c>
/// <c>PipeWriter</c>; <c>FlushAsync</c> completes when bytes hit the pipe's
/// internal buffer, not when the kernel reports a TCP failure. So a downed
/// upstream is only observed at the next read — i.e. <c>CompleteAsync</c>'s
/// <c>ReceiveEndOfStreamAsync</c>. The buffer-survival path therefore needs an
/// exception thrown <i>inside</i> the FlushAsync call, which a row-mapper
/// failure provides deterministically.
/// </remarks>
[Collection("Toxiproxy")]
[Trait(Categories.Name, Categories.Chaos)]
public class BulkInsertBufferSurvivalTests : IAsyncLifetime
{
    private readonly ToxiproxyFixture _proxy;
    private readonly ITestOutputHelper _output;

    public BulkInsertBufferSurvivalTests(ToxiproxyFixture proxy, ITestOutputHelper output)
    {
        _proxy = proxy;
        _output = output;
    }

    public Task InitializeAsync() => _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
    public Task DisposeAsync() => _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);

    [Fact]
    public async Task Flush_RowMapperException_BufferRetainsRows()
    {
        // The contract documented at BulkInserter.cs:476–482: any non-
        // cancellation exception during FlushAsync leaves _buffer untouched.
        // A row-mapper that throws inside the extraction phase reliably hits
        // that catch site (network chaos doesn't, because the pipe writer
        // returns "ok" before the kernel can report a TCP failure).
        const int batchSize = 100;
        const int seedRows = 50;
        const int failAt = 25;

        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _proxy.BuildSettings());

        await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
        await conn.OpenAsync();
        var inserter = conn.CreateBulkInserter<FailingRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = batchSize });
        await inserter.InitAsync();

        Exception? caught = null;
        using (FailingRow.WithFailIndex(failAt))
        {
            for (int i = 0; i < seedRows; i++)
                await inserter.AddAsync(new FailingRow { Id = i, Payload = "x" });

            try { await inserter.FlushAsync(); }
            catch (Exception ex) { caught = ex; }
        }

        Assert.NotNull(caught);
        _output.WriteLine($"FlushAsync surfaced: {caught!.GetType().Name}: {caught.Message}");
        _output.WriteLine($"BufferedCount after failure: {inserter.BufferedCount}");

        // The buffer survives. Exact count depends on whether the extractor ran
        // in column-major (process all rows before failing) or row-major (fail
        // halfway), but it must be > 0 — that's the contract.
        Assert.True(inserter.BufferedCount > 0,
            "Non-cancellation flush failure must leave _buffer intact so the caller can retry.");

        // The inserter is not in the cancellation drain path, so _completeStarted
        // should still be false (only the cancellation catch sets it).
        Assert.False(InserterStateInspector.CompleteStarted(inserter),
            "_completeStarted is reserved for the cancellation drain path; an extraction failure must not flip it.");

        try { await inserter.DisposeAsync(); } catch { /* expected: unflushed rows */ }
    }

    [Fact]
    public async Task Flush_AfterTransientNetworkRecovery_OnNewConnection_RetriesUnsentRows()
    {
        // End-to-end recovery: a flush fails partway through, the application
        // opens a fresh connection and retries the unsent rows. Exercises the
        // audit-by-fresh-connection pattern alongside an interrupted insert.
        const int batchSize = 100;
        const int totalRows = 5_000;

        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _proxy.BuildSettings());
        var bigPayload = new string('p', 4096);
        var rows = Enumerable.Range(0, totalRows)
            .Select(i => new StandardRow { Id = i, Payload = bigPayload })
            .ToArray();

        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "bandwidth", "upstream",
            new() { ["rate"] = 128 });
        var injectTask = Task.Run(async () =>
        {
            await Task.Delay(400);
            await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "reset_peer", "downstream",
                new() { ["timeout"] = 0 });
        });

        try
        {
            await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
            await conn.OpenAsync();
            await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = batchSize });
            await inserter.InitAsync();
            foreach (var row in rows)
                await inserter.AddAsync(row);
            await inserter.CompleteAsync();
        }
        catch (Exception ex)
        {
            _output.WriteLine($"First attempt failed (expected): {ex.GetType().Name}");
        }
        finally
        {
            await injectTask;
            await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
        }

        var committedAfterFailure = (int)await harness.CountAsync();
        _output.WriteLine($"Committed after failure: {committedAfterFailure} of {totalRows}");
        Assert.True(committedAfterFailure < totalRows, "Test failed to interrupt the first attempt.");
        Assert.True(committedAfterFailure % batchSize == 0, "Torn batch from first attempt.");

        await using (var retryConn = new ClickHouseConnection(_proxy.BuildSettings()))
        {
            await retryConn.OpenAsync();
            await using var retryInserter = retryConn.CreateBulkInserter<StandardRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = batchSize });
            await retryInserter.InitAsync();
            foreach (var row in rows.Skip(committedAfterFailure))
                await retryInserter.AddAsync(row);
            await retryInserter.CompleteAsync();
        }

        Assert.Equal((ulong)totalRows, await harness.CountAsync());
    }

    // Note: a "CompleteAsync surfaces network failure with bounded commit"
    // test would be redundant — that contract is already pinned by
    // BulkInsertAtomicityTests.Reset_BetweenBlocks_CommittedCountIsAlwaysMultipleOfBatchSize.
    // The "buffer retains rows on FlushAsync exception" path is covered by
    // Flush_RowMapperException_BufferRetainsRows above (network chaos cannot
    // reliably trigger that path because the System.IO.Pipelines PipeWriter
    // returns success before TCP failures are observable).
}
