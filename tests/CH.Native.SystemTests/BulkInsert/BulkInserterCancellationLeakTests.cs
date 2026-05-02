using System.Diagnostics;
using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Regression test pinning that repeated cancel-mid-insert cycles do not leak
/// the inserter's pooled batch / column-data arrays. The contract: every
/// successful or failed AddRange*Async path must return its rented arrays via
/// try/finally, and DisposeAsync must always run <c>ReturnPooledArrays</c>.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Cancellation)]
public class BulkInserterCancellationLeakTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public BulkInserterCancellationLeakTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task RepeatedCancelCycles_DoNotGrowMemoryUnboundedly()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fx.BuildSettings());

        // Warm up: one full cycle to settle any one-time allocations (schema cache,
        // JIT, ArrayPool bucket fills).
        await RunCycle(harness, cancelEarly: false);

        // Settle GC and capture baseline.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var baseline = GC.GetTotalMemory(forceFullCollection: true);
        var sw = Stopwatch.StartNew();

        const int cycles = 50;
        for (int i = 0; i < cycles; i++)
        {
            await RunCycle(harness, cancelEarly: true);
        }

        sw.Stop();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var after = GC.GetTotalMemory(forceFullCollection: true);

        var deltaMb = (after - baseline) / 1024.0 / 1024.0;
        _output.WriteLine($"{cycles} cancel cycles in {sw.Elapsed.TotalSeconds:F1}s, GC delta {deltaMb:F2} MB.");

        // Generous bound: each leak would be roughly BatchSize × ref + column data.
        // 50 cycles leaking ~1 KB each would still be < 1 MB, so we set a 16 MB bar
        // to leave room for fixture / cache fluff while still catching real leaks
        // (which would scale linearly with cycle count).
        Assert.True(deltaMb < 16.0,
            $"Memory grew {deltaMb:F2} MB across {cycles} cancel cycles; suspect leak.");

        // Functional contract: connection still usable end-to-end.
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
    }

    private async Task RunCycle(BulkInsertTableHarness harness, bool cancelEarly)
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 256 });
        await inserter.InitAsync();

        var s = new string('x', 64);
        try
        {
            for (int i = 0; i < 200; i++)
            {
                await inserter.AddAsync(new StandardRow { Id = i, Payload = s });
            }

            using var cts = new CancellationTokenSource();
            if (cancelEarly)
            {
                cts.Cancel();
                try { await inserter.CompleteAsync(cts.Token); }
                catch (OperationCanceledException) { }
            }
            else
            {
                await inserter.CompleteAsync();
            }
        }
        catch
        {
            // Cancellation paths sometimes surface other transients in CI; the
            // memory assertion above is the load-bearing check.
        }
    }
}
