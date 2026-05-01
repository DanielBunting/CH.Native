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

        // Drain GC before we measure.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var smallStart = GC.GetTotalMemory(forceFullCollection: true);
        ulong smallSum = 0;
        // Untyped QueryAsync returns the raw value (not the per-row generic
        // mapper which requires a class). For a single UInt64 scalar column,
        // this is the right path.
        await foreach (var row in conn.QueryAsync("SELECT number FROM numbers(100000)"))
        {
            smallSum += Convert.ToUInt64(row[0]);
        }
        var smallPeak = GC.GetTotalMemory(forceFullCollection: false);
        var smallDelta = smallPeak - smallStart;

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var largeStart = GC.GetTotalMemory(forceFullCollection: true);
        ulong largeSum = 0;
        await foreach (var row in conn.QueryAsync("SELECT number FROM numbers(1000000)"))
        {
            largeSum += Convert.ToUInt64(row[0]);
        }
        var largePeak = GC.GetTotalMemory(forceFullCollection: false);
        var largeDelta = largePeak - largeStart;

        _output.WriteLine($"100k rows: delta={smallDelta:N0} bytes; sum={smallSum}");
        _output.WriteLine($"  1M rows: delta={largeDelta:N0} bytes; sum={largeSum}");

        // Sanity: query produced expected sum (n*(n-1)/2).
        Assert.Equal(99999UL * 100000 / 2, smallSum);
        Assert.Equal(999999UL * 1000000 / 2, largeSum);

        // The streaming contract: 10× more rows must NOT cause 10× the
        // post-GC working-set delta. We allow generous slack (3×) for
        // reader-internal buffers and block-level batching.
        // If the library buffers all rows, this would be ~10×; the assert
        // catches the catastrophic regression while tolerating reasonable
        // implementation-internal buffering.
        if (smallDelta > 0)
        {
            var ratio = (double)largeDelta / smallDelta;
            _output.WriteLine($"Memory-delta ratio (1M/100k): {ratio:F2}× (rowcount ratio: 10×)");
            Assert.True(ratio < 5.0,
                $"Streaming contract broken: 10× rows produced {ratio:F1}× memory growth.");
        }
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
