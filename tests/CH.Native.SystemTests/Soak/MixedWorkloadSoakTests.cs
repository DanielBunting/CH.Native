using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Soak;

/// <summary>
/// Long-running mixed-workload tests that exercise the whole stack continuously and
/// assert resources stay bounded. Driven by the <c>CHNATIVE_SOAK_DURATION</c> env var
/// (TimeSpan, default 00:10:00). Skipped from default runs via the <c>Soak</c> trait.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Soak)]
public class MixedWorkloadSoakTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public MixedWorkloadSoakTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task ReadersAndInsertsConcurrentlyForDuration_StayBounded()
    {
        var duration = SoakDuration.Resolve();
        var deadline = DateTime.UtcNow + duration;

        var table = $"soak_{Guid.NewGuid():N}";
        await using (var setup = new ClickHouseConnection(_fixture.BuildSettings()))
        {
            await setup.OpenAsync();
            await setup.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id UInt64, payload String) ENGINE = MergeTree ORDER BY id");
        }

        var insertCount = 0L;
        var queryCount = 0L;
        long peakWorkingSet = 0;

        using var cts = new CancellationTokenSource(duration + TimeSpan.FromSeconds(30));

        var inserter = Task.Run(async () =>
        {
            await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
            await conn.OpenAsync(cts.Token);
            ulong nextId = 0;
            while (DateTime.UtcNow < deadline)
            {
                var sql = $"INSERT INTO {table} VALUES ({nextId++}, 'x')";
                await conn.ExecuteNonQueryAsync(sql, cancellationToken: cts.Token);
                Interlocked.Increment(ref insertCount);
            }
        }, cts.Token);

        var reader = Task.Run(async () =>
        {
            await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
            await conn.OpenAsync(cts.Token);
            while (DateTime.UtcNow < deadline)
            {
                _ = await conn.ExecuteScalarAsync<ulong>(
                    $"SELECT count() FROM {table}", cancellationToken: cts.Token);
                Interlocked.Increment(ref queryCount);
            }
        }, cts.Token);

        var sampler = Task.Run(async () =>
        {
            while (DateTime.UtcNow < deadline)
            {
                var ws = Environment.WorkingSet;
                if (ws > peakWorkingSet) peakWorkingSet = ws;
                await Task.Delay(1000, cts.Token);
            }
        }, cts.Token);

        await Task.WhenAll(inserter, reader, sampler);

        _output.WriteLine($"Soak duration: {duration}");
        _output.WriteLine($"Inserts:       {insertCount:N0}");
        _output.WriteLine($"Queries:       {queryCount:N0}");
        _output.WriteLine($"Peak WS:       {peakWorkingSet / (1024.0 * 1024.0):F1} MiB");

        // The workload must have made progress — a deadlocked or stuck soak silently
        // returning fast would otherwise pass the heap-ceiling check.
        Assert.True(insertCount > 0, "No inserts made progress during soak.");
        Assert.True(queryCount > 0, "No queries made progress during soak.");

        // After steady-state plus a final compacting GC the heap should be modest.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var totalManaged = GC.GetTotalMemory(forceFullCollection: true);
        _output.WriteLine($"Final managed: {totalManaged / (1024.0 * 1024.0):F1} MiB");

        // Hard ceiling: 256 MiB managed. Real footprint is far below this; the assertion
        // is intentionally loose so it only fires on egregious leaks.
        Assert.True(totalManaged < 256 * 1024 * 1024,
            $"Managed heap grew to {totalManaged:N0} bytes after soak — possible leak.");

        // Working-set ceiling is generous (1 GiB) but bounded — a runaway native leak
        // would blow past this even if managed memory looks fine.
        Assert.True(peakWorkingSet < 1024L * 1024 * 1024,
            $"Working set peaked at {peakWorkingSet:N0} bytes — possible native leak.");

        // Cleanup.
        await using var teardown = new ClickHouseConnection(_fixture.BuildSettings());
        await teardown.OpenAsync();
        await teardown.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
    }
}
