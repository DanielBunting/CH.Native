namespace CH.Native.SystemTests.Allocation;

/// <summary>
/// Measures bytes allocated process-wide across an async or sync workload, after warmup
/// and a forced GC. Uses <see cref="GC.GetTotalAllocatedBytes(bool)"/> rather than the
/// per-thread counter because async workloads resume on arbitrary threadpool threads.
/// Intentionally simple: it is a regression tripwire, not a microbenchmark — use
/// BenchmarkDotNet for absolute numbers.
/// </summary>
internal static class AllocationProbe
{
    public static async Task<long> MeasureAsync(Func<Task> workload, int warmups = 2, int samples = 7)
    {
        for (int i = 0; i < warmups; i++)
            await workload().ConfigureAwait(false);

        long min = long.MaxValue;
        for (int i = 0; i < samples; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            var before = GC.GetTotalAllocatedBytes(precise: true);
            await workload().ConfigureAwait(false);
            var after = GC.GetTotalAllocatedBytes(precise: true);

            min = Math.Min(min, after - before);
        }
        return min;
    }

    public static long Measure(Action workload, int warmups = 2, int samples = 7)
    {
        for (int i = 0; i < warmups; i++)
            workload();

        long min = long.MaxValue;
        for (int i = 0; i < samples; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            var before = GC.GetTotalAllocatedBytes(precise: true);
            workload();
            var after = GC.GetTotalAllocatedBytes(precise: true);

            min = Math.Min(min, after - before);
        }
        return min;
    }
}
