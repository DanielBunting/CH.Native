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
        // To make the cancel reliably fire mid-flight rather than after the whole insert
        // already completed on a fast loopback, we use a payload large enough that the
        // wire write cannot finish inside the cancellation window.
        var table = $"cancel_bi_{Guid.NewGuid():N}";
        await using var setup = new ClickHouseConnection(_fixture.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, payload String) ENGINE = Memory");

        try
        {
            const int rowCount = 200_000;
            const int payloadBytes = 4096; // ~800 MB total; will not finish in 200 ms on loopback
            const int cancelMs = 200;
            var payload = new string('x', payloadBytes);

            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(cancelMs));
            bool observedCancel = false;
            try
            {
                await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
                await conn.OpenAsync(cts.Token);
                await using var inserter = conn.CreateBulkInserter<Row>(table,
                    new BulkInsert.BulkInsertOptions { BatchSize = 1000 });
                await inserter.InitAsync(cts.Token);

                for (int i = 0; i < rowCount; i++)
                    await inserter.AddAsync(new Row { Id = i, Payload = payload }, cts.Token);
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
            _output.WriteLine($"BulkInsert mid-flight cancel: observedCancel={observedCancel}, rows committed={rows}");

            // Cancellation must have actually fired; otherwise the test isn't testing
            // what it claims (e.g. the insert finished before the timer).
            Assert.True(observedCancel,
                "Cancellation never propagated — payload may be too small or timeout too generous; test isn't exercising mid-flight cancel.");

            // And because the protocol is all-or-nothing per INSERT statement, a
            // mid-flight cancel commits exactly zero rows.
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
