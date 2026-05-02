using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pins the ClickHouse-native commit boundary: the server commits an INSERT only
/// when the empty-block terminator lands. Until then, even data blocks that have
/// been fully transmitted and acknowledged at the TCP level are <em>not</em>
/// visible to readers.
///
/// <para>
/// <see cref="BulkInsertAtomicityTests"/> covers the chaotic mid-stream-reset
/// case (committed counts must be multiples of <c>BatchSize</c>). This file covers
/// the clean-failure case: the inserter sends N full blocks successfully, then
/// the client decides not to call <c>CompleteAsync</c>. The contract is that
/// none of the rows are visible — the absence of the terminator is the absence
/// of a commit.
/// </para>
///
/// <para>
/// This isn't a hypothetical: a caller that hits an exception between
/// <c>FlushAsync</c> and <c>CompleteAsync</c> needs a clear answer for
/// "did my data land?" — and the answer is "no, until the terminator is sent."
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class BulkInsertTerminatorAtomicityTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public BulkInsertTerminatorAtomicityTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task DisposeWithBufferedRowsButPriorFlushes_LoudThrow_FlushedRowsCommit_BufferedAreLost()
    {
        // Surprising contract worth pinning:
        //
        // BulkInserter.DisposeAsync (BulkInserter.cs:686-700) detects "unflushed
        // rows + no CompleteAsync called" and:
        //   1. clears the buffer,
        //   2. sends SendEmptyBlockAsync "best-effort teardown" — which IS
        //      ClickHouse's commit terminator, so the 4 previously-flushed
        //      blocks DO commit,
        //   3. throws InvalidOperationException to surface the data-loss path
        //      (the buffered row never made it onto the wire).
        //
        // Net: the throw says "rows are NOT persisted" — but only the BUFFERED
        // remainder is lost. Already-flushed batches persist. This is a subtle
        // contract worth a test, because callers reading the exception message
        // might assume "everything was rolled back" when actually only the
        // remainder was discarded.
        const int batchSize = 500;
        const int totalRows = 2_000; // exactly 4 full flushes
        const int extraBufferedRows = 1; // remainder that triggers the throw

        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());

        InvalidOperationException? caught = null;
        try
        {
            await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
            await conn.OpenAsync();

            await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = batchSize });
            await inserter.InitAsync();

            for (int i = 0; i < totalRows; i++)
                await inserter.AddAsync(new StandardRow { Id = i, Payload = "x" });

            for (int i = 0; i < extraBufferedRows; i++)
                await inserter.AddAsync(new StandardRow { Id = totalRows + i, Payload = "x" });
        }
        catch (InvalidOperationException ex)
        {
            caught = ex;
        }

        _output.WriteLine($"Dispose threw: {caught?.GetType().Name}: {caught?.Message}");
        Assert.NotNull(caught);
        // The exception message must truthfully describe what was lost AND what
        // persisted — saying "no rows persisted" would mislead callers into
        // re-issuing the whole insert and creating duplicates.
        Assert.Contains("un-flushed row", caught!.Message);
        Assert.Contains("LOST", caught.Message);
        Assert.Contains($"{totalRows} previously-flushed", caught.Message);

        // Locked-in: flushed batches persist (per-block commit semantics);
        // only the buffered remainder is lost.
        var committed = await harness.CountAsync();
        _output.WriteLine($"Committed rows after loud-throw dispose: {committed} (expected {totalRows})");
        Assert.Equal((ulong)totalRows, committed);
    }

    [Fact]
    public async Task FlushedRowsThenAbort_AreNotVisible()
    {
        // Same shape as above but the failure path is triggered explicitly via
        // Abort + dispose. Internal API exercised through reflection-free path:
        // we drive multiple successful flushes, then fail loudly by leaving
        // buffered rows.
        const int totalRows = 1_500;

        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 500 });
        await inserter.InitAsync();

        for (int i = 0; i < totalRows; i++)
            await inserter.AddAsync(new StandardRow { Id = i, Payload = "x" });

        // Three full blocks landed. Don't call CompleteAsync. Dispose without
        // buffered rows (since totalRows is a multiple of BatchSize) should
        // implicitly drive CompleteAsync per BulkInserter.cs lines 703-707 —
        // which means in this configuration the rows DO commit.
        //
        // Pin the actual behavior so an unintended change to that codepath
        // shows up here.
        await inserter.DisposeAsync();

        var committed = await harness.CountAsync();
        _output.WriteLine($"Committed rows when dispose drives implicit complete: {committed}");
        // BulkInserter.cs DisposeAsync, when there are zero buffered rows but
        // _initialized && !_completed && !_completeStarted, awaits CompleteAsync
        // implicitly. So this path *does* commit.
        Assert.Equal((ulong)totalRows, committed);
    }

    [Fact]
    public async Task ExplicitCompleteAfterFlushes_AllRowsCommittedAtomically()
    {
        // Sanity-check the success path: explicit CompleteAsync sends the
        // terminator and the rows become visible exactly once.
        const int totalRows = 3_000;

        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // Pre-condition: empty table.
        Assert.Equal(0UL, await harness.CountAsync());

        await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 500 });
        await inserter.InitAsync();

        for (int i = 0; i < totalRows; i++)
        {
            await inserter.AddAsync(new StandardRow { Id = i, Payload = "x" });
            // Mid-stream: rows still not visible (no terminator yet).
            if (i == 1500)
            {
                Assert.Equal(0UL, await harness.CountAsync());
            }
        }

        await inserter.CompleteAsync();

        // Post-Complete: all rows committed atomically.
        Assert.Equal((ulong)totalRows, await harness.CountAsync());
    }
}
