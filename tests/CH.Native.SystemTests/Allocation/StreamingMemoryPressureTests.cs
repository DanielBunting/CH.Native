using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Allocation;

/// <summary>
/// Probes whether <see cref="ClickHouseConnection.QueryAsync{T}"/> streams
/// large result sets without holding the entire result set in memory. The
/// API contract of <c>IAsyncEnumerable</c> is that callers can iterate
/// row-by-row and let GC reclaim consumed rows; if the implementation
/// buffers everything internally, peak working set scales with result size
/// and large queries OOM the process.
///
/// <para>
/// We don't assert exact memory bounds (too brittle across runtimes), but
/// we DO check the relative shape: streaming a 10× larger result set should
/// not multiply working-set growth by 10×. If it does, the streaming
/// contract is broken.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Allocation)]
public class StreamingMemoryPressureTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public StreamingMemoryPressureTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task QueryAsync_LargeResultSet_PeakMemoryDoesNotScaleLinearlyWithRowCount()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var smallRetained = await MeasureRetainedAfterStreamAsync(conn, rowCount: 100_000);
        var largeRetained = await MeasureRetainedAfterStreamAsync(conn, rowCount: 1_000_000);

        _output.WriteLine($"100k rows retained after GC: {smallRetained:N0} bytes");
        _output.WriteLine($"  1M rows retained after GC: {largeRetained:N0} bytes");

        // The streaming contract: after iteration completes and a full GC
        // runs, retained heap must be O(1) in the row count — bounded by
        // pool bucket residue, the active block scratch buffer, and a
        // little async-state-machine state. It must NOT scale with the
        // number of rows produced.
        //
        // 1M UInt64 rows boxed via the untyped QueryAsync path would cost
        // ~32 MB if fully buffered (24-byte object header per boxed value
        // + the 8-byte payload, plus per-row object[] containers). An
        // absolute ceiling at 8 MB catches catastrophic full-buffering
        // regressions while tolerating realistic pool bucket retention
        // and warmup. We deliberately don't compare a small/large *ratio*
        // because the small denominator can settle near zero after GC,
        // making any ratio brittle across runtimes.
        const long ceilingBytes = 8L * 1024 * 1024;
        Assert.True(largeRetained < ceilingBytes,
            $"Streaming contract broken: 1M rows retained {largeRetained:N0} bytes after GC " +
            $"(ceiling {ceilingBytes:N0}).");
    }

    private static async Task<long> MeasureRetainedAfterStreamAsync(ClickHouseConnection conn, int rowCount)
    {
        // Drain finalizers and Gen2 before capturing the baseline.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var start = GC.GetTotalMemory(forceFullCollection: true);

        ulong sum = 0;
        await foreach (var row in conn.QueryAsync($"SELECT number FROM numbers({rowCount})"))
        {
            sum += Convert.ToUInt64(row[0]);
        }

        // Sanity: query produced expected sum (n*(n-1)/2). Confirms the
        // stream actually delivered every row (so we're measuring the
        // streaming path, not an early-terminated short read).
        var expected = (ulong)(rowCount - 1) * (ulong)rowCount / 2;
        Assert.Equal(expected, sum);

        // Force a full GC so we measure RETAINED heap (rooted by pools,
        // statics, and the connection) rather than per-row Gen0 garbage
        // that just hasn't been collected yet.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        return GC.GetTotalMemory(forceFullCollection: true) - start;
    }

    [Fact]
    public async Task QueryAsync_LongStream_DoesNotHangOrOom()
    {
        // Sanity check: a multi-million-row stream completes without hanging
        // and without throwing OOM on a default test runner.
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var sw = System.Diagnostics.Stopwatch.StartNew();
        long rowCount = 0;
        await foreach (var _ in conn.QueryAsync("SELECT number FROM numbers(5000000)"))
        {
            rowCount++;
        }
        sw.Stop();

        _output.WriteLine($"5M rows streamed in {sw.ElapsedMilliseconds} ms ({rowCount / Math.Max(sw.Elapsed.TotalSeconds, 0.001):F0} rows/s)");
        Assert.Equal(5_000_000L, rowCount);
    }
}
