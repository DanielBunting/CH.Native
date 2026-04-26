using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Application-side failures during row extraction: the row-mapper delegate
/// throws, an oversized payload causes OOM, or a non-nullable column receives
/// null. These are not network failures — they originate in the user's code or
/// data, and the inserter must surface them without silently mangling state.
/// </summary>
[Collection("SingleNode")]
public class BulkInsertExtractionFailureTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public BulkInsertExtractionFailureTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task Extraction_DelegateThrows_ConnectionRemainsUsable()
    {
        // The row-mapper throws on a specific row index. The exception type
        // (FailingRowException) propagates without wrapping. The connection must
        // be usable for subsequent operations — a poisoned wire here would
        // surface as an InvalidOperationException on SELECT 1.
        const int batchSize = 1000;
        const int failAt = 50;

        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        Exception? caught = null;
        using (FailingRow.WithFailIndex(failAt))
        {
            await using var inserter = conn.CreateBulkInserter<FailingRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = batchSize });
            await inserter.InitAsync();

            try
            {
                // Stay below batchSize so no auto-flush masks the throw site.
                for (int i = 0; i < 100; i++)
                    await inserter.AddAsync(new FailingRow { Id = i, Payload = "data" });
                await inserter.FlushAsync();
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            // After a row-mapper failure on the flush path, the inserter is
            // mid-INSERT on the wire. Abort via Dispose; the buffer survives the
            // throw, so we explicitly call Abort() through a fresh complete-skip
            // path. The cleanest way to dispose is to acknowledge the broken
            // state by calling Abort indirectly: there is no public Abort, so
            // we accept the implicit-complete throw on dispose.
            try { await inserter.DisposeAsync(); }
            catch (InvalidOperationException) { /* expected: unflushed rows */ }
        }

        Assert.NotNull(caught);
        // FailingRowException may be wrapped by the column-extractor pipeline;
        // either it surfaces directly or as InnerException. Both are acceptable
        // — what matters is that the failure is surfaced, not swallowed.
        var underlying = caught is FailingRowException ? caught : caught!.InnerException;
        Assert.True(underlying is FailingRowException,
            $"Row-mapper failure should propagate FailingRowException; got {caught!.GetType().Name}: {caught.Message}");

        // The connection must be reusable. A new INSERT-bearing query proves the
        // wire is not stuck mid-INSERT (which would manifest as a protocol error
        // or a hang). Use a simple SELECT — Tier A does not assert what AddAsync
        // does on a poisoned inserter, only that the underlying connection is
        // sound. If this throws, the inserter's failure has corrupted the wire.
        var probe = await conn.ExecuteScalarAsync<int>("SELECT 1");
        Assert.Equal(1, probe);
    }

    [Fact]
    public async Task Extraction_DelegateThrows_FreshInserter_Succeeds()
    {
        // After a row-mapper failure, a freshly-constructed inserter on a fresh
        // connection must work end-to-end. This is the recovery contract from
        // the application's perspective.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());

        // Provoke a failure on a doomed inserter.
        using (FailingRow.WithFailIndex(5))
        {
            await using var doomedConn = new ClickHouseConnection(_fixture.BuildSettings());
            await doomedConn.OpenAsync();
            await using var doomed = doomedConn.CreateBulkInserter<FailingRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 1000 });
            await doomed.InitAsync();
            try
            {
                for (int i = 0; i < 10; i++)
                    await doomed.AddAsync(new FailingRow { Id = i, Payload = "x" });
                await doomed.FlushAsync();
            }
            catch { /* expected */ }
            try { await doomed.DisposeAsync(); } catch { /* expected: unflushed */ }
        }

        // Fresh inserter — succeeds.
        const int totalRows = 250;
        await using (var freshConn = new ClickHouseConnection(_fixture.BuildSettings()))
        {
            await freshConn.OpenAsync();
            await using var fresh = freshConn.CreateBulkInserter<StandardRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 100 });
            await fresh.InitAsync();
            for (int i = 0; i < totalRows; i++)
                await fresh.AddAsync(new StandardRow { Id = i, Payload = "ok" });
            await fresh.CompleteAsync();
        }

        Assert.Equal((ulong)totalRows, await harness.CountAsync());
    }

    [Fact]
    public async Task Extraction_LargePayload_RoundTrips_OrFailsLoudly()
    {
        // 4 MiB row — large but not OOM-territory on any reasonable runner.
        // Pin: either the inserter accepts it and round-trips correctly, or it
        // surfaces a typed error. A silent truncation is a bug.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());

        var bigPayload = new string('q', 4 * 1024 * 1024);

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 5 });
        await inserter.InitAsync();

        for (int i = 0; i < 3; i++)
            await inserter.AddAsync(new StandardRow { Id = i, Payload = bigPayload });
        await inserter.CompleteAsync();

        await using var auditConn = new ClickHouseConnection(_fixture.BuildSettings());
        await auditConn.OpenAsync();
        var lengthSum = await auditConn.ExecuteScalarAsync<ulong>(
            $"SELECT sum(length(payload)) FROM {harness.TableName}");
        Assert.Equal(3UL * (ulong)bigPayload.Length, lengthSum);
    }

    [Fact]
    public async Task Extraction_NullForNonNullableStringColumn_FreshInserter_Succeeds()
    {
        // Companion to Extraction_NullForNonNullableStringColumn_ThrowsCleanly.
        // The throw inside the extractor leaves the original connection in a
        // mid-INSERT state (the same contract as Extraction_DelegateThrows
        // failures and Dispose_AfterCompleteThrew_DoesNotRetryWire — failed
        // completes do NOT retry the wire). The recovery contract from the
        // application's perspective is: a freshly-constructed inserter on a
        // fresh connection succeeds end-to-end and persists the rows.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());

        // Provoke the extractor failure on a doomed connection.
        await using (var doomedConn = new ClickHouseConnection(_fixture.BuildSettings()))
        {
            await doomedConn.OpenAsync();
            await using var doomed = doomedConn.CreateBulkInserter<StandardRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 10 });
            await doomed.InitAsync();
            try
            {
                await doomed.AddAsync(new StandardRow { Id = 1, Payload = null! });
                await doomed.CompleteAsync();
            }
            catch { /* expected */ }
        }

        // Fresh connection — succeeds.
        const int totalRows = 50;
        await using (var freshConn = new ClickHouseConnection(_fixture.BuildSettings()))
        {
            await freshConn.OpenAsync();
            await using var fresh = freshConn.CreateBulkInserter<StandardRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 10 });
            await fresh.InitAsync();
            for (int i = 0; i < totalRows; i++)
                await fresh.AddAsync(new StandardRow { Id = i, Payload = "ok" });
            await fresh.CompleteAsync();
        }

        Assert.Equal((ulong)totalRows, await harness.CountAsync());
    }

    [Fact]
    public async Task Extraction_NullForNullableStringColumn_LandsAsNull()
    {
        // Regression guard: the Bug 3 fix only touches the non-nullable branch
        // of StringExtractor (and FixedStringExtractor). The nullable branch
        // must still accept null and write the bitmap byte that marks the row
        // as null on the server. Without this test, a future change that
        // collapses both branches could regress Nullable(String) silently.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fixture.BuildSettings(),
            columnDdl: "id Int32, payload Nullable(String)");

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await using var inserter = conn.CreateBulkInserter<NullablePayloadRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 10 });
        await inserter.InitAsync();

        await inserter.AddAsync(new NullablePayloadRow { Id = 1, Payload = null });
        await inserter.AddAsync(new NullablePayloadRow { Id = 2, Payload = "present" });
        await inserter.CompleteAsync();

        await using var auditConn = new ClickHouseConnection(_fixture.BuildSettings());
        await auditConn.OpenAsync();
        var nullCount = await auditConn.ExecuteScalarAsync<ulong>(
            $"SELECT count() FROM {harness.TableName} WHERE payload IS NULL");
        Assert.Equal(1UL, nullCount);
    }

    [Fact]
    public async Task Extraction_NullForNonNullableFixedStringColumn_ThrowsCleanly()
    {
        // FixedStringExtractor parallel to the StringExtractor failing test.
        // Without the fix, FixedStringExtractor.WriteFixedString would write
        // a zero-padded buffer of length 8 — visually indistinguishable from
        // a real all-zero payload. The fix raises the same explicit throw.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fixture.BuildSettings(),
            columnDdl: "id Int32, payload FixedString(8)");

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 10 });
        await inserter.InitAsync();

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = null! });
            await inserter.CompleteAsync();
        });

        try { await inserter.DisposeAsync(); } catch { /* tolerate */ }
    }

    [Fact]
    public async Task Extraction_NullForNonNullableStringColumn_FallbackPath_ThrowsCleanly()
    {
        // The original Bug 3 fix touched only the direct-extractor path. This
        // test forces the boxed/fallback path (the direct-extractor factory
        // throws NotSupportedException on Array(Int32) properties, which flips
        // _useDirectPath = false in BulkInserter), then sends null on a
        // sibling String column. Without the wrapper-substitution redesign on
        // the IColumnWriter side, the null would silently land as "" via the
        // old `?? string.Empty` in StringColumnWriter.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fixture.BuildSettings(),
            columnDdl: "id Int32, tags Array(Int32), payload String");

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        var inserter = conn.CreateBulkInserter<RowWithArrayAndString>(harness.TableName,
            new BulkInsertOptions { BatchSize = 10 });
        await inserter.InitAsync();

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await inserter.AddAsync(new RowWithArrayAndString
            {
                Id = 1,
                Tags = new[] { 1, 2 },
                Payload = null!,
            });
            await inserter.CompleteAsync();
        });

        try { await inserter.DisposeAsync(); } catch { /* tolerate */ }
    }

    [Fact]
    public async Task Extraction_NullForNullableStringColumn_FallbackPath_LandsAsNull()
    {
        // Companion regression-guard: Nullable(String) on the fallback path
        // must still land as a real NULL. Verifies NullableRefColumnWriter's
        // substitution path: bitmap byte 1 + empty-string placeholder under
        // the bitmap. The server reads the bitmap and ignores the placeholder.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fixture.BuildSettings(),
            columnDdl: "id Int32, tags Array(Int32), payload Nullable(String)");

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await using var inserter = conn.CreateBulkInserter<RowWithArrayAndNullableString>(harness.TableName,
            new BulkInsertOptions { BatchSize = 10 });
        await inserter.InitAsync();

        await inserter.AddAsync(new RowWithArrayAndNullableString
        {
            Id = 1,
            Tags = new[] { 1, 2 },
            Payload = null,
        });
        await inserter.AddAsync(new RowWithArrayAndNullableString
        {
            Id = 2,
            Tags = new[] { 3 },
            Payload = "present",
        });
        await inserter.CompleteAsync();

        await using var auditConn = new ClickHouseConnection(_fixture.BuildSettings());
        await auditConn.OpenAsync();
        var nullCount = await auditConn.ExecuteScalarAsync<ulong>(
            $"SELECT count() FROM {harness.TableName} WHERE payload IS NULL");
        Assert.Equal(1UL, nullCount);
    }

    [Fact]
    public async Task Extraction_NullForNonNullableStringColumn_ThrowsCleanly()
    {
        // BUG-EXPOSING TEST. Currently FAILS against main.
        //
        // The column is declared `String` (non-nullable). Sending null is a
        // type-system violation — the inserter MUST reject it, not silently
        // coerce it to an empty string or write whatever bytes happen to be
        // around. Silent acceptance here means an entire production batch can
        // land with the wrong values for any column where the source POCO had
        // a null; the discrepancy only surfaces during analysis, when the bad
        // data is already commingled with good.
        //
        // Today the library accepts null and writes *something* to the wire.
        // The fix is a null-check inside the column extractor for non-nullable
        // String (and equivalent non-nullable types) so the failure surfaces
        // synchronously at AddAsync — same pattern as a missing column.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 10 });
        await inserter.InitAsync();

        // The load-bearing assertion: null into a non-nullable column must
        // throw somewhere in the AddAsync→FlushAsync→CompleteAsync pipeline.
        // Anything else is silent data corruption.
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = null! });
            await inserter.CompleteAsync();
        });

        try { await inserter.DisposeAsync(); } catch { /* tolerate */ }
    }
}
