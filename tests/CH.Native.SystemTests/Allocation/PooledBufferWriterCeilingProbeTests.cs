using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Allocation;

/// <summary>
/// Probes the 1MB <c>PooledBufferWriter</c> retain ceiling (surface-area §2.2).
/// Workloads with reliably-larger blocks pay re-rent overhead; this probe
/// pins the current allocation profile so a regression — or a future
/// "make it a setting" change — registers as observable change.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Allocation)]
public class PooledBufferWriterCeilingProbeTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public PooledBufferWriterCeilingProbeTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task SustainedBulkInsert_FixedBatchSize_AllocationsStayBoundedAcrossBlocks()
    {
        // Run a sustained 50-block insert at fixed batch size. The total
        // allocations measured must be bounded — a regression that
        // disabled buffer pooling would scale linearly with block count.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "ceiling");

        const int batchSize = 1_000;
        const int blocks = 50;
        var rows = Enumerable.Range(0, batchSize * blocks)
            .Select(i => new StandardRow { Id = i, Payload = new string('x', 256) })
            .ToList();

        // Warm.
        await Insert(harness.TableName, rows.Take(batchSize).ToList(), batchSize);

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            await Insert(harness.TableName, rows, batchSize);
        });

        _output.WriteLine($"sustained {blocks} blocks ({batchSize} rows each, 256-byte payload): {bytes:N0} bytes");

        // Hard ceiling: 1MB ceiling × 50 re-rents would still keep
        // allocations under ~150 MB even in pathological scenarios.
        // Tightening this beyond a generous envelope is BenchmarkDotNet
        // territory; the test is a tripwire against orders-of-magnitude
        // regressions.
        Assert.InRange(bytes, 0, 200_000_000);
    }

    [Fact]
    public async Task BlockSizesAcrossThreshold_SmallAndAboveCeiling_BothFunction()
    {
        // 500-row blocks: well below 1 MB. 4000-row blocks with wide
        // payloads: above the ceiling. Both must function; the ceiling
        // affects amortisation, not correctness.
        await using var smallHarness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "ceil_small");
        await using var bigHarness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "ceil_big");

        var small = Enumerable.Range(0, 5_000)
            .Select(i => new StandardRow { Id = i, Payload = "x" }).ToList();
        var big = Enumerable.Range(0, 4_000)
            .Select(i => new StandardRow { Id = i, Payload = new string('x', 1024) }).ToList();

        await Insert(smallHarness.TableName, small, batchSize: 500);
        await Insert(bigHarness.TableName, big, batchSize: 4_000);

        Assert.Equal(5_000UL, await smallHarness.CountAsync());
        Assert.Equal(4_000UL, await bigHarness.CountAsync());
    }

    private async Task Insert(string table, IReadOnlyCollection<StandardRow> rows, int batchSize)
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        await conn.BulkInsertAsync(table, rows,
            new BulkInsertOptions { BatchSize = batchSize });
    }
}
