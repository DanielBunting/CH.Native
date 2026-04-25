using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Cancellation;

/// <summary>
/// Ensures cancellation leaves the system in a clean state: the connection is reusable,
/// pool stats don't leak, and explicit <c>KillQueryAsync</c> actually terminates the
/// server-side query.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Cancellation)]
public class CancelRecoveryTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public CancelRecoveryTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task Cancel_Then_Reuse_Connection_Many_Times()
    {
        const int cycles = 25;
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        int actuallyCancelled = 0;
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < cycles; i++)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
            bool threw = false;
            try
            {
                _ = await conn.ExecuteScalarAsync<ulong>(
                    "SELECT count() FROM numbers(1000000000)",
                    cancellationToken: cts.Token);
            }
            catch (OperationCanceledException) { threw = true; }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                // Some implementations wrap cancellation; accept any non-test exception
                // but still count it as an observed cancellation.
                threw = true;
            }
            if (threw) actuallyCancelled++;

            // Immediately after cancellation the connection MUST be reusable.
            var ok = await conn.ExecuteScalarAsync<int>("SELECT 1");
            Assert.Equal(1, ok);
        }
        sw.Stop();
        _output.WriteLine($"{cycles} cancel/reuse cycles in {sw.Elapsed.TotalSeconds:F1}s; actually cancelled = {actuallyCancelled}");

        // The 100 ms CT against a 1B-row count must trigger cancellation in the vast
        // majority of cycles; if 0 cycles cancelled we aren't testing what we claim.
        Assert.True(actuallyCancelled >= cycles / 2,
            $"Expected ≥ {cycles / 2} cancellations, observed {actuallyCancelled} — server may be too fast or token not honoured.");
    }

    [Fact]
    public async Task KillQueryAsync_TerminatesLongRunningQuery()
    {
        await using var driver = new ClickHouseConnection(_fixture.BuildSettings());
        await driver.OpenAsync();

        // KillQueryAsync requires GUID-formatted IDs.
        var queryId = Guid.NewGuid().ToString();
        var queryTask = driver.ExecuteScalarAsync<ulong>(
            "SELECT count() FROM numbers(10000000000)",
            queryId: queryId);

        // Give the server a moment to register the query.
        await Task.Delay(300);

        await using var killer = new ClickHouseConnection(_fixture.BuildSettings());
        await killer.OpenAsync();
        await killer.KillQueryAsync(queryId);

        // The driver task should fault or return within a reasonable window.
        var completed = await Task.WhenAny(queryTask, Task.Delay(TimeSpan.FromSeconds(10)));
        Assert.True(completed == queryTask,
            "KillQueryAsync did not terminate the long-running query within 10s.");

        // The query should be gone from system.processes within 2s.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        ulong stillThere = 1;
        while (DateTime.UtcNow < deadline)
        {
            stillThere = await killer.ExecuteScalarAsync<ulong>(
                $"SELECT count() FROM system.processes WHERE query_id = '{queryId}'");
            if (stillThere == 0) break;
            await Task.Delay(100);
        }
        Assert.Equal(0UL, stillThere);
    }

    [Fact]
    public async Task BulkInsert_CancelMidFlight_ServerStaysHealthy()
    {
        // Pin the all-or-nothing contract: the ClickHouse native protocol commits an
        // INSERT statement only when the client sends the empty terminator block.
        // If cancellation fires before CompleteAsync's terminator lands, the server
        // discards every block it accumulated for this insert (MergeTreeSink::~MergeTreeSink
        // -> partition.temp_part->cancel(); MemorySink drops new_blocks). So a successful
        // mid-flight cancel must produce EXACTLY zero rows committed.
        //
        // Deterministic trigger: cancel from inside the loop at a fixed row index, not
        // a wall-clock timer. A timer-based cancel is host-speed-dependent — on a fast
        // loopback the insert can finish before the timer fires, making the test pass
        // for the wrong reason (terminator already sent, all rows committed) or fail
        // because cancel was observed only at dispose-time. Tripping the CTS at row N
        // guarantees the cancel lands strictly before CompleteAsync.
        var table = $"cancel_bi_{Guid.NewGuid():N}";
        await using var setup = new ClickHouseConnection(_fixture.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, payload String) ENGINE = Memory");

        try
        {
            const int rowCount = 200_000;
            const int cancelAtRow = 50_000;
            const int payloadBytes = 4096;
            var payload = new string('x', payloadBytes);

            using var cts = new CancellationTokenSource();
            bool observedCancel = false;
            bool reachedComplete = false;
            try
            {
                await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
                await conn.OpenAsync(cts.Token);
                await using var inserter = conn.CreateBulkInserter<Row>(table,
                    new BulkInsert.BulkInsertOptions { BatchSize = 1000 });
                await inserter.InitAsync(cts.Token);

                for (int i = 0; i < rowCount; i++)
                {
                    if (i == cancelAtRow)
                        cts.Cancel();
                    await inserter.AddAsync(new Row { Id = i, Payload = payload }, cts.Token);
                }
                reachedComplete = true;
                await inserter.CompleteAsync(cts.Token);
            }
            catch (OperationCanceledException) { observedCancel = true; }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                // Library may surface a wrapped variant (e.g. InvalidOperationException
                // from Dispose noting unflushed rows after the inner cancel); accept any
                // non-test exception as evidence that the cancel propagated.
                observedCancel = true;
            }

            // After the cancelled session is gone, the SERVER should be healthy.
            await using var fresh = new ClickHouseConnection(_fixture.BuildSettings());
            await fresh.OpenAsync();
            var v = await fresh.ExecuteScalarAsync<int>("SELECT 1");
            Assert.Equal(1, v);

            var rows = await fresh.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            _output.WriteLine($"BulkInsert mid-flight cancel: observedCancel={observedCancel}, reachedComplete={reachedComplete}, rows committed={rows}");

            // The throw must have come from inside the AddAsync loop, before CompleteAsync.
            // If reachedComplete is true the cancel landed too late to be "mid-flight" —
            // the test isn't exercising what it claims.
            Assert.False(reachedComplete,
                "Cancel landed after the AddAsync loop — CompleteAsync was reached, so this isn't a mid-flight cancel.");
            Assert.True(observedCancel,
                "Cancellation never propagated out of the AddAsync loop.");

            // And because the protocol is all-or-nothing per INSERT statement, a
            // mid-flight cancel commits exactly zero rows.
            Assert.Equal(0UL, rows);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task BulkInsert_CancelImmediatelyAfterBoundaryFlush_CommitsZero()
    {
        // Variant of BulkInsert_CancelMidFlight_ServerStaysHealthy that exercises a
        // different wire state at cancel time. With a post-step trigger, AddAsync at
        // i = cancelAtRow runs to completion first — the boundary flush (50th batch)
        // lands fully on the wire, AddAsync returns, THEN cts.Cancel() fires. The
        // next iteration's AddAsync sees the dead token and enters the slow path
        // with the most recent batch already fully written and no in-flight
        // server-bound write. Atomicity contract still demands zero committed rows.
        var table = $"cancel_bi_post_{Guid.NewGuid():N}";
        await using var setup = new ClickHouseConnection(_fixture.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, payload String) ENGINE = Memory");

        try
        {
            const int rowCount = 200_000;
            const int cancelAtRow = 49_999; // last row of 50th batch — AddAsync triggers flush
            const int payloadBytes = 4096;
            var payload = new string('x', payloadBytes);

            using var cts = new CancellationTokenSource();
            bool observedCancel = false;
            bool reachedComplete = false;
            try
            {
                await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
                await conn.OpenAsync(cts.Token);
                await using var inserter = conn.CreateBulkInserter<Row>(table,
                    new BulkInsert.BulkInsertOptions { BatchSize = 1000 });
                await inserter.InitAsync(cts.Token);

                for (int i = 0; i < rowCount; i++)
                {
                    await inserter.AddAsync(new Row { Id = i, Payload = payload }, cts.Token);
                    // Post-step trigger: at i == 49_999 the AddAsync above has just
                    // completed the 50th batch flush. Cancel fires AFTER the flush
                    // returns, so the next iteration enters AddAsync with a dead
                    // token and the wire is quiescent.
                    if (i == cancelAtRow)
                        cts.Cancel();
                }
                reachedComplete = true;
                await inserter.CompleteAsync(cts.Token);
            }
            catch (OperationCanceledException) { observedCancel = true; }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException) { observedCancel = true; }

            await using var fresh = new ClickHouseConnection(_fixture.BuildSettings());
            await fresh.OpenAsync();
            Assert.Equal(1, await fresh.ExecuteScalarAsync<int>("SELECT 1"));

            var rows = await fresh.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            _output.WriteLine($"Post-flush cancel: observedCancel={observedCancel}, reachedComplete={reachedComplete}, rows committed={rows}");

            Assert.False(reachedComplete,
                "Cancel landed after the AddAsync loop — CompleteAsync was reached, so this isn't a mid-flight cancel.");
            Assert.True(observedCancel,
                "Cancellation never propagated out of the AddAsync loop.");
            Assert.Equal(0UL, rows);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task BulkInsert_CancelMidBatch_NonBoundary_CommitsZero()
    {
        // Cancel at a non-batch-boundary row: 50 batches flushed, 500 rows still
        // sitting in the local List<T> buffer when the cancel observation runs.
        // Those 500 rows were never on the wire; the 50_000 already-flushed rows
        // were on the wire but never committed (no terminator). Both must end at
        // zero. Pins that BulkInserter doesn't accidentally drain a buffered
        // remainder during the cancel path.
        var table = $"cancel_bi_mid_{Guid.NewGuid():N}";
        await using var setup = new ClickHouseConnection(_fixture.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, payload String) ENGINE = Memory");

        try
        {
            const int rowCount = 200_000;
            const int cancelAtRow = 50_500; // 50 batches flushed, 500 rows buffered
            const int payloadBytes = 4096;
            var payload = new string('x', payloadBytes);

            using var cts = new CancellationTokenSource();
            bool observedCancel = false;
            bool reachedComplete = false;
            int bufferAtCancel = -1;
            try
            {
                await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
                await conn.OpenAsync(cts.Token);
                await using var inserter = conn.CreateBulkInserter<Row>(table,
                    new BulkInsert.BulkInsertOptions { BatchSize = 1000 });
                await inserter.InitAsync(cts.Token);

                for (int i = 0; i < rowCount; i++)
                {
                    if (i == cancelAtRow)
                    {
                        // Snapshot BEFORE Cancel — Abort() clears the buffer in the
                        // slow path, so an after-the-fact read would always see 0.
                        bufferAtCancel = inserter.BufferedCount;
                        cts.Cancel();
                    }
                    await inserter.AddAsync(new Row { Id = i, Payload = payload }, cts.Token);
                }
                reachedComplete = true;
                await inserter.CompleteAsync(cts.Token);
            }
            catch (OperationCanceledException) { observedCancel = true; }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException) { observedCancel = true; }

            await using var fresh = new ClickHouseConnection(_fixture.BuildSettings());
            await fresh.OpenAsync();
            Assert.Equal(1, await fresh.ExecuteScalarAsync<int>("SELECT 1"));

            var rows = await fresh.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            _output.WriteLine($"Mid-batch cancel: observedCancel={observedCancel}, reachedComplete={reachedComplete}, buffered at cancel={bufferAtCancel}, rows committed={rows}");

            Assert.False(reachedComplete,
                "Cancel landed after the AddAsync loop — CompleteAsync was reached, so this isn't a mid-flight cancel.");
            Assert.True(observedCancel,
                "Cancellation never propagated out of the AddAsync loop.");
            // 500 rows were locally buffered at cancel time. If a future change to
            // AddAsync's flush-trigger logic shifts the boundary, this drifts and
            // we want the test to fail loudly so the new behaviour is reviewed.
            Assert.Equal(500, bufferAtCancel);
            Assert.Equal(0UL, rows);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task BulkInsert_CancelMidFlight_SameConnection_StaysReusable()
    {
        // Sister to BulkInsert_CancelMidFlight_ServerStaysHealthy. That one verifies
        // server health on a FRESH connection — necessary but insufficient. This test
        // pins the drain-realignment claim end-to-end: after ObserveCancellationSlowPathAsync
        // runs SendCancelAsync → DrainAfterCancellationAsync, the SAME connection that
        // owned the cancelled insert must accept a follow-up scalar query AND a fresh
        // BulkInsert that completes successfully. If the drain left bytes on the wire,
        // the second insert would either hang or read garbled response bytes.
        var table = $"cancel_reuse_{Guid.NewGuid():N}";
        await using var setup = new ClickHouseConnection(_fixture.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, payload String) ENGINE = Memory");

        try
        {
            const int rowCount = 200_000;
            const int cancelAtRow = 50_000;
            const int payloadBytes = 4096;
            const int reuseRowCount = 1_000;
            var payload = new string('x', payloadBytes);

            // Hold ONE connection for the whole test — this is the contract under audit.
            await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
            await conn.OpenAsync();

            using var cts = new CancellationTokenSource();
            bool observedCancel = false;
            bool reachedComplete = false;
            try
            {
                await using var inserter = conn.CreateBulkInserter<Row>(table,
                    new BulkInsert.BulkInsertOptions { BatchSize = 1000 });
                await inserter.InitAsync(cts.Token);

                for (int i = 0; i < rowCount; i++)
                {
                    if (i == cancelAtRow)
                        cts.Cancel();
                    await inserter.AddAsync(new Row { Id = i, Payload = payload }, cts.Token);
                }
                reachedComplete = true;
                await inserter.CompleteAsync(cts.Token);
            }
            catch (OperationCanceledException) { observedCancel = true; }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException) { observedCancel = true; }

            Assert.False(reachedComplete,
                "Cancel landed after the AddAsync loop — CompleteAsync was reached, so this isn't a mid-flight cancel.");
            Assert.True(observedCancel,
                "Cancellation never propagated out of the AddAsync loop.");

            // 1) Same connection still answers a trivial scalar — proves the drain
            //    advanced the pipe reader to a clean message boundary.
            var v = await conn.ExecuteScalarAsync<int>("SELECT 1");
            Assert.Equal(1, v);

            // 2) Same connection accepts a NEW BulkInserter and finishes a clean
            //    insert. This is the real witness of wire realignment — the second
            //    inserter sends its own INSERT query, reads schema, flushes batches,
            //    and reads the end-of-stream ack on a connection whose previous
            //    state machine ran through the cancel-and-drain path.
            await using (var inserter2 = conn.CreateBulkInserter<Row>(table,
                new BulkInsert.BulkInsertOptions { BatchSize = 500 }))
            {
                await inserter2.InitAsync();
                for (int i = 0; i < reuseRowCount; i++)
                    await inserter2.AddAsync(new Row { Id = i, Payload = "y" });
                await inserter2.CompleteAsync();
            }

            // The cancelled insert contributed zero rows; only the reuse insert's
            // rows should be present.
            var rows = await conn.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            _output.WriteLine($"Same-conn reuse: observedCancel={observedCancel}, reachedComplete={reachedComplete}, rows committed={rows} (expected {reuseRowCount})");
            Assert.Equal((ulong)reuseRowCount, rows);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task BulkInsert_CancelBeforeInit_ThrowsCleanly_NoServerSideQuery()
    {
        // Pre-INSERT cancel: token is already dead at the moment InitAsync starts.
        // BulkInserter.InitAsync's first action is a plain ThrowIfCancellationRequested
        // — NOT ObserveCancellationAsync — because nothing has been sent on the wire
        // yet (the INSERT query hasn't been issued, so there's nothing to cancel
        // server-side and a Cancel packet here would be wire garbage). This test
        // pins that boundary: no INSERT query reaches system.processes, the table
        // stays empty, and the same connection remains usable.
        //
        // The comment at BulkInserter.cs:140-145 is load-bearing — a future refactor
        // that "unifies" the cancellation paths could regress this. The test catches
        // the regression at the integration boundary.
        var table = $"cancel_preinit_{Guid.NewGuid():N}";
        await using var setup = new ClickHouseConnection(_fixture.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, payload String) ENGINE = Memory");

        try
        {
            await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
            await conn.OpenAsync();

            using var cts = new CancellationTokenSource();
            cts.Cancel(); // dead at entry

            await using var inserter = conn.CreateBulkInserter<Row>(table,
                new BulkInsert.BulkInsertOptions { BatchSize = 1000 });

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => inserter.InitAsync(cts.Token));

            // Same connection: scalar still works.
            Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));

            // No INSERT query should have been registered server-side. Match only
            // INSERT INTO {table} (avoid the self-referential LIKE pattern from
            // CancelDuringRoundTripTests.cs:179-188). Poll briefly for race tolerance.
            var deadline = DateTime.UtcNow.AddSeconds(2);
            ulong stuckQueries = 1;
            while (DateTime.UtcNow < deadline)
            {
                stuckQueries = await conn.ExecuteScalarAsync<ulong>(
                    $"SELECT count() FROM system.processes WHERE query LIKE 'INSERT INTO {table}%'");
                if (stuckQueries == 0) break;
                await Task.Delay(100);
            }
            Assert.Equal(0UL, stuckQueries);

            var rows = await conn.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            _output.WriteLine($"Pre-init cancel: stuck queries={stuckQueries}, rows committed={rows}");
            Assert.Equal(0UL, rows);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private class Row
    {
        [Mapping.ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }
        [Mapping.ClickHouseColumn(Name = "payload", Order = 1)]
        public string Payload { get; set; } = "";
    }
}
