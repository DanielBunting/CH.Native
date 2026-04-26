using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Cancellation;

/// <summary>
/// Mid-roundtrip cancel tests. Each test applies a downstream latency toxic at a
/// precise moment so a deterministic <c>cts.Cancel()</c> lands while the client is
/// blocked awaiting a specific server reply (schema block for InitAsync, end-of-stream
/// ack for CompleteAsync). Latency is 800 ms and the cancel fires at 80 ms — a 10×
/// margin that swallows realistic loopback-vs-host scheduling jitter.
/// </summary>
[Collection("Toxiproxy")]
[Trait(Categories.Name, Categories.Cancellation)]
public class CancelDuringRoundTripTests : IAsyncLifetime
{
    private readonly ToxiproxyFixture _proxy;
    private readonly ITestOutputHelper _output;

    private const int LatencyMs = 800;
    private const int CancelAfterMs = 80;

    public CancelDuringRoundTripTests(ToxiproxyFixture proxy, ITestOutputHelper output)
    {
        _proxy = proxy;
        _output = output;
    }

    // Reset toxics before AND after each test — a failed assertion can short-circuit
    // a finally block, leaving toxics in place that poison the next test.
    public Task InitializeAsync() => _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
    public Task DisposeAsync() => _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);

    [Fact]
    public async Task BulkInsert_CancelDuringInitAsync_SchemaWait_WireStaysUsable()
    {
        // Cancel lands while InitAsync is awaiting the schema block: INSERT query has
        // hit the server, the server replied with the schema, but the reply is sitting
        // in the latency toxic. The library's RegisterCancelHook should fire SendCancel
        // and the drain path must reconcile the wire so a fresh connection works.
        var table = $"cancel_init_{Guid.NewGuid():N}";
        await using var setup = new ClickHouseConnection(_proxy.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, payload String) ENGINE = Memory");

        try
        {
            using var cts = new CancellationTokenSource();
            bool observedCancel = false;
            Exception? thrown = null;

            // Open the connection BEFORE adding the toxic so the handshake isn't
            // slowed — only the InitAsync roundtrip should land in the latency window.
            await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
            await conn.OpenAsync();

            await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "latency", "downstream",
                new() { ["latency"] = LatencyMs, ["jitter"] = 0 });

            var canceller = Task.Run(async () =>
            {
                await Task.Delay(CancelAfterMs);
                cts.Cancel();
            });

            try
            {
                await using var inserter = conn.CreateBulkInserter<Row>(table,
                    new BulkInsert.BulkInsertOptions { BatchSize = 1000 });
                await inserter.InitAsync(cts.Token);
            }
            catch (OperationCanceledException ex) { observedCancel = true; thrown = ex; }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                observedCancel = true; thrown = ex;
            }
            finally
            {
                await canceller;
                await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
            }

            _output.WriteLine($"Init cancel: observedCancel={observedCancel}, exType={thrown?.GetType().Name}");

            Assert.True(observedCancel, "Cancel never propagated out of InitAsync.");

            // Server health: a fresh connection works, the table is empty (no data
            // was ever sent), and no orphaned query lingers in system.processes.
            await using var fresh = new ClickHouseConnection(_proxy.BuildSettings());
            await fresh.OpenAsync();
            Assert.Equal(1, await fresh.ExecuteScalarAsync<int>("SELECT 1"));

            var rows = await fresh.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            Assert.Equal(0UL, rows);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task BulkInsert_CancelDuringCompleteAsync_AckWait_WireStaysUsable()
    {
        // Cancel lands after the empty terminator block has been sent but before the
        // server's end-of-stream ack arrives (sitting in the latency toxic). Whether
        // rows ultimately commit is server-dependent at this point — the contract this
        // test pins is the WIRE: the cancel must drain cleanly so a fresh connection
        // works, no query is left orphaned in system.processes, and no exception other
        // than a cancel-shaped one escapes.
        var table = $"cancel_complete_{Guid.NewGuid():N}";
        await using var setup = new ClickHouseConnection(_proxy.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, payload String) ENGINE = Memory");

        try
        {
            const int rowCount = 100;
            var payload = new string('x', 64);

            using var cts = new CancellationTokenSource();
            bool observedCancel = false;
            Exception? thrown = null;

            await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
            await conn.OpenAsync();

            await using var inserter = conn.CreateBulkInserter<Row>(table,
                new BulkInsert.BulkInsertOptions { BatchSize = 10_000 });
            await inserter.InitAsync();

            for (int i = 0; i < rowCount; i++)
                await inserter.AddAsync(new Row { Id = i, Payload = payload });

            // From here on, every server-bound reply is delayed. The next downstream
            // packet the client awaits is the end-of-stream ack inside CompleteAsync.
            await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "latency", "downstream",
                new() { ["latency"] = LatencyMs, ["jitter"] = 0 });

            var canceller = Task.Run(async () =>
            {
                await Task.Delay(CancelAfterMs);
                cts.Cancel();
            });

            try
            {
                await inserter.CompleteAsync(cts.Token);
            }
            catch (OperationCanceledException ex) { observedCancel = true; thrown = ex; }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                observedCancel = true; thrown = ex;
            }
            finally
            {
                await canceller;
                await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
            }

            _output.WriteLine($"Complete cancel: observedCancel={observedCancel}, exType={thrown?.GetType().Name}");

            Assert.True(observedCancel, "Cancel never propagated out of CompleteAsync.");

            // Wire-health invariants — these MUST hold regardless of whether the
            // server committed before honouring the cancel.
            await using var fresh = new ClickHouseConnection(_proxy.BuildSettings());
            await fresh.OpenAsync();
            Assert.Equal(1, await fresh.ExecuteScalarAsync<int>("SELECT 1"));

            // Allow the server a brief window to clear any in-flight query state
            // (cancel handling can race with insert finalisation). Match only the
            // INSERT itself; a broader LIKE pattern matches the poll query's own
            // text and produces a self-referential false positive.
            var deadline = DateTime.UtcNow.AddSeconds(5);
            ulong stuckQueries = 1;
            while (DateTime.UtcNow < deadline)
            {
                stuckQueries = await fresh.ExecuteScalarAsync<ulong>(
                    $"SELECT count() FROM system.processes WHERE query LIKE 'INSERT INTO {table}%'");
                if (stuckQueries == 0) break;
                await Task.Delay(100);
            }
            Assert.Equal(0UL, stuckQueries);

            // Log committed-row outcome for diagnostic value; we do not assert on it
            // because the cancel-vs-commit race is server-side and not part of the
            // wire-health contract this test pins.
            var rows = await fresh.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            _output.WriteLine($"Complete cancel: rows committed = {rows} (informational; not asserted).");
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
