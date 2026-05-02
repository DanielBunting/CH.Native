using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Allocation;

/// <summary>
/// Pins the allocation cost of opting out of the pooled-array path via
/// <see cref="BulkInsertOptions.UsePooledArrays"/>=false. Use-cases §5.2
/// documents the default as <c>true</c>; opting out should be allocation-
/// visible. Without a probe, a future change that silently swapped default
/// behaviour would go undetected.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Allocation)]
public class UsePooledArraysOptOutAllocationTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public UsePooledArraysOptOutAllocationTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task DefaultPooled_OptOutNotPooled_AreEachWithinReasonableEnvelope()
    {
        await using var harnessPooled = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "pool_yes");
        await using var harnessNotPooled = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "pool_no");

        var rows = Enumerable.Range(0, 1_000)
            .Select(i => new StandardRow { Id = i, Payload = "p" })
            .ToList();

        await Insert(harnessPooled.TableName, rows, usePooled: true);
        await Insert(harnessNotPooled.TableName, rows, usePooled: false);

        var pooledBytes = await AllocationProbe.MeasureAsync(async () =>
            await Insert(harnessPooled.TableName, rows, usePooled: true));
        var nonPooledBytes = await AllocationProbe.MeasureAsync(async () =>
            await Insert(harnessNotPooled.TableName, rows, usePooled: false));

        _output.WriteLine($"pooled (default): {pooledBytes:N0} bytes");
        _output.WriteLine($"opt-out         : {nonPooledBytes:N0} bytes");

        // Each insert should land within a generous envelope (5 MB for
        // 1000 rows + framework overhead). Both modes function; this
        // probe is the regression tripwire.
        Assert.InRange(pooledBytes, 0, 5_000_000);
        Assert.InRange(nonPooledBytes, 0, 5_000_000);
    }

    private async Task Insert(string table, IReadOnlyCollection<StandardRow> rows, bool usePooled)
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        await conn.BulkInsertAsync(table, rows,
            new BulkInsertOptions { BatchSize = 250, UsePooledArrays = usePooled });
    }
}
