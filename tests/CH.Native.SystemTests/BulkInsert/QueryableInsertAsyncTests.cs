using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Linq;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// System-level pins for the new <c>connection.Table&lt;T&gt;().InsertAsync(...)</c>
/// surface. Verifies parity with the explicit Init/Add/Complete lifecycle,
/// behavior under cancellation, fan-out from a pooled data source, and error
/// surfacing against an unknown table — the same contracts asserted for the
/// existing <c>BulkInsertAsync</c> wrapper.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class QueryableInsertAsyncTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public QueryableInsertAsyncTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task TableInsertAsync_HappyPath_ProducesSameRowCount_AsBulkInsertAsync()
    {
        // Pin: Table<T>().InsertAsync and BulkInsertAsync share the same
        // underlying lifecycle. Two independent inserts of the same payload
        // must yield the same row count — proving the queryable wrapper
        // doesn't drop / duplicate rows.
        await using var queryableHarness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "qins_q");
        await using var directHarness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "qins_d");

        var rows = Enumerable.Range(0, 1_000)
            .Select(i => new StandardRow { Id = i, Payload = "p" })
            .ToList();

        await using (var conn = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await conn.OpenAsync();
            await conn.Table<StandardRow>(queryableHarness.TableName).InsertAsync(rows);
        }

        await using (var conn = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await conn.OpenAsync();
            await conn.BulkInsertAsync(directHarness.TableName, rows);
        }

        Assert.Equal(1_000UL, await queryableHarness.CountAsync());
        Assert.Equal(await queryableHarness.CountAsync(), await directHarness.CountAsync());
    }

    [Fact]
    public async Task TableInsertAsync_AsyncStream_CancellationMidFlight_RethrowsOperationCanceledException()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "qins_cancel");
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        using var cts = new CancellationTokenSource();

        async IAsyncEnumerable<StandardRow> Rows()
        {
            for (var i = 0; i < 50_000; i++)
            {
                if (i == 100) cts.Cancel();
                await Task.Yield();
                yield return new StandardRow { Id = i, Payload = "p" };
            }
        }

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            conn.Table<StandardRow>(harness.TableName).InsertAsync(
                Rows(),
                new BulkInsertOptions { BatchSize = 50 },
                cts.Token));
    }

    [Fact]
    public async Task TableInsertAsync_AgainstNonExistentTable_SurfacesServerError()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var bogus = $"never_existed_{Guid.NewGuid():N}";
        var rows = new[] { new StandardRow { Id = 1, Payload = "x" } };

        var ex = await Assert.ThrowsAnyAsync<Exception>(() =>
            conn.Table<StandardRow>(bogus).InsertAsync(rows));

        _output.WriteLine($"Table<T>.InsertAsync error: {ex.GetType().Name}: {ex.Message}");
        Assert.IsAssignableFrom<ClickHouseServerException>(ex);
    }

    [Fact]
    public async Task DataSourceTableInsertAsync_FanOut_AllRowsLand_PoolReturnsToIdle()
    {
        // Pin the rent-per-call ergonomic at scale. Sixteen concurrent inserts,
        // each rents from the pool, runs an independent INSERT, returns. After
        // WhenAll, every row must be persisted and Busy must be zero — the
        // pool's invariant after a successful fan-out.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "qins_fanout");
        await using var ds = new ClickHouseDataSource(_fx.BuildSettings());

        const int workers = 16;
        const int rowsPerWorker = 250;

        var tasks = Enumerable.Range(0, workers).Select(async w =>
        {
            var rows = Enumerable.Range(0, rowsPerWorker)
                .Select(i => new StandardRow { Id = w * rowsPerWorker + i, Payload = $"w{w}" });
            await ds.Table<StandardRow>(harness.TableName).InsertAsync(rows);
        });
        await Task.WhenAll(tasks);

        var stats = ds.GetStatistics();
        _output.WriteLine($"After fan-out: {stats}");
        Assert.Equal(0, stats.Busy);
        Assert.True(stats.TotalRentsServed >= workers);
        Assert.Equal((ulong)(workers * rowsPerWorker), await harness.CountAsync());
    }

    [Fact]
    public async Task DataSourceTableInsertAsync_AfterError_PoolStaysHealthy()
    {
        // Pin: a server-rejected insert (e.g. unknown table) must release the
        // rented connection back to the pool's accounting (or discard it
        // cleanly), not leave it stuck busy. The next rent must succeed.
        await using var ds = new ClickHouseDataSource(_fx.BuildSettings());
        var bogus = $"never_existed_{Guid.NewGuid():N}";

        await Assert.ThrowsAnyAsync<ClickHouseServerException>(() =>
            ds.Table<StandardRow>(bogus).InsertAsync(new StandardRow { Id = 1, Payload = "x" }));

        Assert.Equal(0, ds.GetStatistics().Busy);

        // A subsequent rent on a real table must work — proves the pool isn't
        // poisoned by the failed rent.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "qins_after_err");
        await ds.Table<StandardRow>(harness.TableName).InsertAsync(new StandardRow { Id = 1, Payload = "ok" });
        Assert.Equal(1UL, await harness.CountAsync());
    }
}
