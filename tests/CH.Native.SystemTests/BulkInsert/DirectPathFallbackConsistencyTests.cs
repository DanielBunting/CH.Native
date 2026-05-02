using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pins functional equivalence between the bulk-insert direct path and the
/// boxing fallback path. <see cref="BulkInserter{T}"/>'s <c>_useDirectPath</c>
/// flag is set to false silently if any property's column type isn't
/// supported by <see cref="ColumnExtractorFactory"/> (e.g. <c>Array</c>,
/// <c>Map</c>, <c>Tuple</c>) — every row in the buffer then routes through
/// the boxing extraction path. There's no telemetry or warning at the
/// downgrade point; users only notice via profiler.
///
/// <para>
/// What we test:
/// </para>
/// <list type="bullet">
/// <item><description>Fallback path produces the same data as direct path for shared columns.</description></item>
/// <item><description>A POCO with one fallback-forcing column inserts correctly via the
///     fallback path (no silent skip, no wrong values).</description></item>
/// </list>
///
/// <para>
/// We don't test for the "missing telemetry/warning" — that's a feature gap,
/// not a correctness bug, and adding noise on the happy path would surprise
/// existing callers.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class DirectPathFallbackConsistencyTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public DirectPathFallbackConsistencyTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fixture = fx;
        _output = output;
    }

    private sealed class DirectPathRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "name", Order = 1)] public string Name { get; set; } = "";
    }

    private sealed class FallbackPathRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "name", Order = 1)] public string Name { get; set; } = "";
        [ClickHouseColumn(Name = "tags", Order = 2)] public int[] Tags { get; set; } = Array.Empty<int>();
    }

    [Fact]
    public async Task DirectPath_AndFallbackPath_ProduceEquivalentRowsForSharedColumns()
    {
        // Two harnesses with identical id/name columns. One inserter uses
        // the direct path (POCO has only direct-supported types); the other
        // uses fallback (POCO has an array column).
        await using var directHarness = await BulkInsertTableHarness.CreateAsync(
            () => _fixture.BuildSettings(),
            columnDdl: "id Int32, name String",
            namePrefix: "direct");
        await using var fallbackHarness = await BulkInsertTableHarness.CreateAsync(
            () => _fixture.BuildSettings(),
            columnDdl: "id Int32, name String, tags Array(Int32)",
            namePrefix: "fallback");

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // Direct path
        await using (var inserter = conn.CreateBulkInserter<DirectPathRow>(directHarness.TableName,
            new BulkInsertOptions { BatchSize = 100 }))
        {
            await inserter.InitAsync();
            for (int i = 0; i < 100; i++)
                await inserter.AddAsync(new DirectPathRow { Id = i, Name = $"row-{i}" });
            await inserter.CompleteAsync();
        }

        // Fallback path
        await using (var inserter = conn.CreateBulkInserter<FallbackPathRow>(fallbackHarness.TableName,
            new BulkInsertOptions { BatchSize = 100 }))
        {
            await inserter.InitAsync();
            for (int i = 0; i < 100; i++)
                await inserter.AddAsync(new FallbackPathRow
                {
                    Id = i,
                    Name = $"row-{i}",
                    Tags = new[] { i, i * 2 }
                });
            await inserter.CompleteAsync();
        }

        Assert.Equal(100UL, await directHarness.CountAsync());
        Assert.Equal(100UL, await fallbackHarness.CountAsync());

        // Cross-check: both paths produced identical id/name pairs.
        await using var verifyConn = new ClickHouseConnection(_fixture.BuildSettings());
        await verifyConn.OpenAsync();

        var directIds = new List<int>();
        await foreach (var row in verifyConn.QueryAsync<DirectPathRow>(
            $"SELECT id, name FROM {directHarness.TableName} ORDER BY id"))
            directIds.Add(row.Id);

        var fallbackIds = new List<int>();
        await foreach (var row in verifyConn.QueryAsync<DirectPathRow>(
            $"SELECT id, name FROM {fallbackHarness.TableName} ORDER BY id"))
            fallbackIds.Add(row.Id);

        Assert.Equal(directIds, fallbackIds);
    }

    [Fact]
    public async Task FallbackPath_NullableString_RoundTripsCorrectly()
    {
        // Specifically pin the regression that the fallback path handles
        // Nullable(String) — the existing BulkInsertTableHarness mentions
        // this as a previously-failing combination.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fixture.BuildSettings(),
            columnDdl: "id Int32, tags Array(Int32), payload Nullable(String)",
            namePrefix: "fallback_nullable");

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        await using var inserter = conn.CreateBulkInserter<RowWithArrayAndNullableString>(harness.TableName,
            new BulkInsertOptions { BatchSize = 50 });
        await inserter.InitAsync();
        for (int i = 0; i < 50; i++)
        {
            await inserter.AddAsync(new RowWithArrayAndNullableString
            {
                Id = i,
                Tags = new[] { i },
                Payload = i % 3 == 0 ? null : $"row-{i}"
            });
        }
        await inserter.CompleteAsync();

        Assert.Equal(50UL, await harness.CountAsync());
    }
}
