using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Linq;

/// <summary>
/// System-level pins for <c>dataSource.Table&lt;T&gt;()</c> LINQ reads — the
/// rent-per-enumeration path that the <c>AsyncQueryableExtensions</c>
/// aggregates were rewired to use. Distinct from the integration-suite
/// equivalents in that the SystemTests fixture spins a single shared node
/// and asserts pool-side invariants under load.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class DataSourceTableLinqSystemTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public DataSourceTableLinqSystemTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task DataSourceQueryable_Aggregates_AllRouteThroughLeaseHelper_AndReleaseConnections()
    {
        // Pin every aggregate that AsyncQueryableExtensions rewired to lease
        // through ClickHouseQueryContext.AcquireConnectionAsync. After running
        // each in turn against a data-source-bound queryable, Busy must be
        // zero — proving the lease helper's owning-dispose path returns the
        // connection in every case.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "ds_linq_agg");
        await using var ds = new ClickHouseDataSource(_fx.BuildSettings());

        // Seed once.
        await ds.Table<StandardRow>(harness.TableName).InsertAsync(
            Enumerable.Range(0, 200).Select(i => new StandardRow { Id = i, Payload = $"p{i}" }));

        // Each call rents and returns. We don't assert specific values across
        // every aggregate (other tests cover correctness) — the load-bearing
        // assertion here is that the pool stays clean after the full sweep.
        var q = ds.Table<StandardRow>(harness.TableName);

        _ = await q.ToListAsync();
        _ = await q.OrderBy(r => r.Id).FirstAsync();
        _ = await q.Where(r => r.Id == -1).FirstOrDefaultAsync();
        _ = await q.CountAsync();
        _ = await q.CountAsync(r => r.Id % 2 == 0);
        _ = await q.LongCountAsync();
        _ = await q.AnyAsync();
        _ = await q.AnyAsync(r => r.Id == 50);
        _ = await q.AllAsync(r => r.Id >= 0);
        _ = await q.SumAsync(r => r.Id);
        _ = await q.AverageAsync(r => r.Id);
        _ = await q.MinAsync(r => r.Id);
        _ = await q.MaxAsync(r => r.Id);

        var stats = ds.GetStatistics();
        _output.WriteLine($"After full aggregate sweep: {stats}");
        Assert.Equal(0, stats.Busy);
    }

    [Fact]
    public async Task DataSourceQueryable_Concurrent_Aggregates_RentIndependentConnections()
    {
        // Pin: concurrent aggregates over a data-source-bound queryable each
        // rent independently. The pool's MaxPoolSize gates how many physical
        // connections actually open simultaneously — we don't assert a specific
        // peak (depends on pool config), only that all queries complete and
        // the pool ends idle.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "ds_linq_conc");
        await using var ds = new ClickHouseDataSource(_fx.BuildSettings());

        await ds.Table<StandardRow>(harness.TableName).InsertAsync(
            Enumerable.Range(0, 1_000).Select(i => new StandardRow { Id = i, Payload = "p" }));

        const int parallelism = 16;
        var tasks = Enumerable.Range(0, parallelism).Select(_ => Task.Run(async () =>
        {
            var q = ds.Table<StandardRow>(harness.TableName);
            return await q.CountAsync();
        })).ToArray();

        var counts = await Task.WhenAll(tasks);
        Assert.All(counts, c => Assert.Equal(1_000, c));
        Assert.Equal(0, ds.GetStatistics().Busy);
    }

    [Fact]
    public async Task DataSourceQueryable_Enumeration_RoundTripsSeededRows()
    {
        // End-to-end happy path: write through the data-source-bound handle,
        // read back through the same handle. The read goes through
        // ClickHouseQueryable<T>.GetAsyncEnumerator, which is the other
        // lease-using path (separate from the aggregate extensions).
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "ds_linq_rt");
        await using var ds = new ClickHouseDataSource(_fx.BuildSettings());

        var seeded = Enumerable.Range(0, 50)
            .Select(i => new StandardRow { Id = i, Payload = $"p{i}" })
            .ToList();
        await ds.Table<StandardRow>(harness.TableName).InsertAsync(seeded);

        var readBack = await ds.Table<StandardRow>(harness.TableName)
            .OrderBy(r => r.Id)
            .ToListAsync();

        Assert.Equal(50, readBack.Count);
        Assert.Equal(seeded.Select(r => r.Id), readBack.Select(r => r.Id));
        Assert.Equal(0, ds.GetStatistics().Busy);
    }
}
