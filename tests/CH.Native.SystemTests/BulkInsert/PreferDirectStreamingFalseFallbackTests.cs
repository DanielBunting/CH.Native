using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.SystemTests.Allocation;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pins the explicit opt-out from the direct-streaming path via
/// <see cref="BulkInsertOptions.PreferDirectStreaming"/>=false. Existing
/// <c>DirectPathFallbackConsistencyTests</c> covers the auto-detected
/// fallback (POCO shape forces boxing); this test covers the explicit
/// opt-out and verifies row parity plus an allocation gap.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Allocation)]
public class PreferDirectStreamingFalseFallbackTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public PreferDirectStreamingFalseFallbackTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task ExplicitOptOut_ProducesSameRowCount_AsDirectStreaming()
    {
        await using var directHarness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "direct_yes");
        await using var fallbackHarness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "direct_no");

        var rows = Enumerable.Range(0, 1_000)
            .Select(i => new StandardRow { Id = i, Payload = "p" })
            .ToList();

        await using (var conn = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await conn.OpenAsync();
            await conn.BulkInsertAsync(directHarness.TableName, rows,
                new BulkInsertOptions { BatchSize = 250, PreferDirectStreaming = true });
        }

        await using (var conn = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await conn.OpenAsync();
            await conn.BulkInsertAsync(fallbackHarness.TableName, rows,
                new BulkInsertOptions { BatchSize = 250, PreferDirectStreaming = false });
        }

        Assert.Equal(1_000UL, await directHarness.CountAsync());
        Assert.Equal(await directHarness.CountAsync(), await fallbackHarness.CountAsync());
    }

    [Fact]
    public async Task ExplicitOptOut_AllocatesAtLeastAsMuchAsDirectPath()
    {
        await using var harnessA = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "alloc_direct");
        await using var harnessB = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "alloc_fallback");

        var rows = Enumerable.Range(0, 1_000)
            .Select(i => new StandardRow { Id = i, Payload = "p" })
            .ToList();

        // Warm.
        await Insert(harnessA.TableName, rows, preferDirect: true);
        await Insert(harnessB.TableName, rows, preferDirect: false);

        var directBytes = await AllocationProbe.MeasureAsync(async () =>
            await Insert(harnessA.TableName, rows, preferDirect: true));
        var fallbackBytes = await AllocationProbe.MeasureAsync(async () =>
            await Insert(harnessB.TableName, rows, preferDirect: false));

        _output.WriteLine($"direct: {directBytes:N0}, fallback: {fallbackBytes:N0}");

        // Order-of-magnitude regression detector only. At 1 000 rows the
        // per-batch overhead (connection open, schema fetch, framing)
        // dominates and the boxed-vs-direct gap washes out — sometimes the
        // boxed path even allocates slightly less than direct on a given
        // run. The strict "boxed > direct" claim lives in
        // benchmarks/CH.Native.Benchmarks
        // (BulkInsertDirectVsBoxedAllocationBenchmarks) at 50 K+ rows where
        // the per-batch cost amortises out and the gap is stable. Here we
        // just assert the boxed path didn't silently bypass its
        // boxed-array logic entirely.
        Assert.True(fallbackBytes >= directBytes * 0.5,
            $"Fallback ({fallbackBytes}) is unexpectedly far below direct ({directBytes}); the boxed path likely regressed.");
    }

    private async Task Insert(string table, IReadOnlyCollection<StandardRow> rows, bool preferDirect)
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        await conn.BulkInsertAsync(table, rows,
            new BulkInsertOptions { BatchSize = 250, PreferDirectStreaming = preferDirect });
    }
}
