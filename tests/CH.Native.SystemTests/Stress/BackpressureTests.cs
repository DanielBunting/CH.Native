using CH.Native.Compression;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Stress;

/// <summary>
/// Validates the library's headline streaming guarantee: a slow consumer must NOT cause
/// unbounded buffer growth on the client side. Stream millions of rows while inserting
/// artificial delays between row reads, and assert managed heap stays bounded.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class BackpressureTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public BackpressureTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public Task SlowConsumer_Uncompressed_HeapStaysBounded() =>
        RunAsync(compress: false, method: CompressionMethod.Lz4);

    [Fact]
    public Task SlowConsumer_Lz4_HeapStaysBounded() =>
        RunAsync(compress: true, method: CompressionMethod.Lz4);

    [Fact]
    public Task SlowConsumer_Zstd_HeapStaysBounded() =>
        RunAsync(compress: true, method: CompressionMethod.Zstd);

    private async Task RunAsync(bool compress, CompressionMethod method)
    {
        const int rows = 5_000_000;
        // Per-mode heap ceilings: uncompressed should be tight; compressed pays for
        // decompression buffers. These pin tighter than the previous flat 256 MiB cap.
        long heapCeilingBytes = compress ? 128L * 1024 * 1024 : 64L * 1024 * 1024;

        var settings = _fixture.BuildSettings(b =>
        {
            b.WithCompression(compress);
            if (compress) b.WithCompressionMethod(method);
        });

        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        long peakHeap = 0;
        long peakWorkingSet = 0;
        var streamed = 0L;

        var sampler = Task.Run(async () =>
        {
            while (Volatile.Read(ref _samplerStop) == 0)
            {
                var heap = GC.GetTotalMemory(forceFullCollection: false);
                if (heap > peakHeap) peakHeap = heap;
                var ws = Environment.WorkingSet;
                if (ws > peakWorkingSet) peakWorkingSet = ws;
                await Task.Delay(250);
            }
        });

        try
        {
            await foreach (var row in conn.QueryAsync(
                $"SELECT number FROM numbers({rows})"))
            {
                _ = row.GetFieldValue<ulong>(0);
                streamed++;
                // Periodic 100-µs nap simulates a slow downstream sink (Kafka / S3 / network).
                if (streamed % 1000 == 0)
                    await Task.Delay(0); // yield without sleeping
                if (streamed % 250_000 == 0)
                    await Task.Delay(TimeSpan.FromMilliseconds(2));
            }
        }
        finally
        {
            Interlocked.Exchange(ref _samplerStop, 1);
            await sampler;
            Interlocked.Exchange(ref _samplerStop, 0);
        }

        Assert.Equal(rows, streamed);

        _output.WriteLine($"compress={compress} method={method}");
        _output.WriteLine($"  rows streamed: {streamed:N0}");
        _output.WriteLine($"  peak managed heap: {peakHeap / (1024.0 * 1024.0):F1} MiB");
        _output.WriteLine($"  peak working set:  {peakWorkingSet / (1024.0 * 1024.0):F1} MiB");

        Assert.True(peakHeap < heapCeilingBytes,
            $"Heap grew to {peakHeap:N0} bytes (> {heapCeilingBytes:N0}). Backpressure may be broken.");
    }

    private long _samplerStop;
}
