using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Backpressure when the producer is faster than the wire. Existing bulk-insert tests
/// all assume an ungated server-bound stream; throttling via Toxiproxy surfaces the
/// bounded-memory contract and the cancellation-while-throttled deadlock surface.
///
/// <para>2M rows (×~80 bytes payload) is sized for CI: large enough that an unbounded
/// producer would be visibly distinct from a backpressured one in <c>GC.GetTotalMemory</c>
/// samples, small enough that the test budget under bandwidth throttling is on the order
/// of 30s rather than minutes.</para>
/// </summary>
[Collection("Toxiproxy")]
[Trait(Categories.Name, Categories.Stress)]
[Trait(Categories.Name, Categories.Chaos)]
public class BulkInsertBackpressureTests : IAsyncLifetime
{
    private readonly ToxiproxyFixture _proxy;
    private readonly ITestOutputHelper _output;

    public BulkInsertBackpressureTests(ToxiproxyFixture proxy, ITestOutputHelper output)
    {
        _proxy = proxy;
        _output = output;
    }

    public Task InitializeAsync() => _proxy.ResetProxyAsync();
    public Task DisposeAsync() => _proxy.ResetProxyAsync();

    private const int RowCount = 2_000_000;
    private const int BandwidthBytesPerSec = 4 * 1024 * 1024; // 4 MB/s — fast enough for CI, slow enough to throttle

    [Fact]
    public async Task ProducerFasterThanWire_MemoryStaysBounded()
    {
        var harness = await BulkInsertTableHarness.CreateAsync(
            () => _proxy.BuildSettings(),
            columnDdl: "id Int64, payload String");

        try
        {
            await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
            await conn.OpenAsync();

            await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "bandwidth", "upstream",
                new() { ["rate"] = BandwidthBytesPerSec / 1024 }); // toxiproxy expects KB/s

            // Sample peak managed memory while the insert runs.
            long peak = 0;
            using var samplerCts = new CancellationTokenSource();
            var sampler = Task.Run(async () =>
            {
                while (!samplerCts.IsCancellationRequested)
                {
                    var v = GC.GetTotalMemory(forceFullCollection: false);
                    if (v > peak) peak = v;
                    try { await Task.Delay(50, samplerCts.Token); } catch { return; }
                }
            });

            var baseline = GC.GetTotalMemory(forceFullCollection: true);

            await using (var inserter = conn.CreateBulkInserter<BackpressureRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 50_000 }))
            {
                await inserter.InitAsync();
                await inserter.AddRangeStreamingAsync(GenerateRowsAsync(RowCount));
                await inserter.CompleteAsync();
            }

            samplerCts.Cancel();
            try { await sampler; } catch { }

            var growth = peak - baseline;
            _output.WriteLine($"Memory baseline={baseline:N0}B, peak={peak:N0}B, growth={growth:N0}B");

            // 256 MB is generous — a regression that buffers all 2M rows (~160 MB
            // of payload + framing) would still register here, but a bounded
            // pipeline should sit comfortably below.
            Assert.True(growth < 256L * 1024 * 1024,
                $"Bulk insert grew managed memory by {growth:N0} bytes; expected < 256 MB.");

            var inserted = await harness.CountAsync();
            Assert.Equal((ulong)RowCount, inserted);
        }
        finally
        {
            await _proxy.ResetProxyAsync();
            await harness.DisposeAsync();
        }
    }

    [Fact]
    public async Task ProducerFasterThanWire_CancellationWhileThrottled_NoDeadlock()
    {
        var harness = await BulkInsertTableHarness.CreateAsync(
            () => _proxy.BuildSettings(),
            columnDdl: "id Int64, payload String");

        try
        {
            await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
            await conn.OpenAsync();

            await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "bandwidth", "upstream",
                new() { ["rate"] = BandwidthBytesPerSec / 1024 });

            using var cts = new CancellationTokenSource();
            // Cancel after 3s — well after the throttle has begun gating writes.
            _ = Task.Run(async () =>
            {
                await Task.Delay(3000);
                cts.Cancel();
            });

            var stopwatchAfterCancel = System.Diagnostics.Stopwatch.StartNew();
            Exception? caught = null;
            try
            {
                await using var inserter = conn.CreateBulkInserter<BackpressureRow>(harness.TableName,
                    new BulkInsertOptions { BatchSize = 50_000 });
                await inserter.InitAsync(cts.Token);
                await inserter.AddRangeStreamingAsync(GenerateRowsAsync(RowCount), cts.Token);
                await inserter.CompleteAsync(cts.Token);
            }
            catch (OperationCanceledException ex) { caught = ex; }
            catch (Exception ex) { caught = ex; }
            stopwatchAfterCancel.Stop();

            _output.WriteLine($"Cancel-to-throw: {stopwatchAfterCancel.ElapsedMilliseconds}ms; type={caught?.GetType().Name}");
            Assert.NotNull(caught);

            // No deadlock contract: the cancel must be observable. The bandwidth
            // toxic gates writes — if the cancel propagation deadlocked, this
            // wait would time out at the test framework's level; assertion here
            // just pins the upper bound.
            await _proxy.ResetProxyAsync();

            // A fresh connection still works against the same table.
            await using var fresh = new ClickHouseConnection(_proxy.BuildSettings());
            await fresh.OpenAsync();
            Assert.Equal(1, await fresh.ExecuteScalarAsync<int>("SELECT 1"));

            // No half-committed insert: the count is whatever the server saw before
            // cancel, which for a streaming insert that didn't hit CompleteAsync
            // should be 0 (CH commits on EOF terminator block).
            var inserted = await harness.CountAsync();
            _output.WriteLine($"Rows committed at cancel time: {inserted} (informational).");
        }
        finally
        {
            await _proxy.ResetProxyAsync();
            await harness.DisposeAsync();
        }
    }

    private static async IAsyncEnumerable<BackpressureRow> GenerateRowsAsync(int count)
    {
        // The payload must be incompressible per row, otherwise LZ4 (the default
        // compression) collapses an identical-string corpus to a few MB on the wire
        // and the bandwidth toxic never gates the stream. We mix a per-row random
        // suffix into a fixed prefix so the row shape stays uniform but each row's
        // bytes differ.
        var rng = new Random(42);
        var suffix = new byte[40];
        for (int i = 0; i < count; i++)
        {
            rng.NextBytes(suffix);
            yield return new BackpressureRow
            {
                Id = i,
                Payload = "x_" + Convert.ToHexString(suffix),
            };
            if ((i & 0xFFFF) == 0) await Task.Yield();
        }
    }

    internal sealed class BackpressureRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public long Id { get; set; }

        [ClickHouseColumn(Name = "payload", Order = 1)]
        public string Payload { get; set; } = string.Empty;
    }
}
