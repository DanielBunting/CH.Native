using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pins the IEnumerable&lt;T&gt; overload of <c>AddRangeStreamingAsync</c>.
/// The IAsyncEnumerable overload is heavily exercised by the chaos suite;
/// this overload's parity with it is what callers rely on when their data
/// source is a synchronous iterator.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class AddRangeStreamingSyncEnumerableTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public AddRangeStreamingSyncEnumerableTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task AddRangeStreamingAsync_IEnumerable_BuffersAndFlushesAcrossBatchBoundary()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "sync_enum");
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        IEnumerable<StandardRow> Rows()
        {
            for (int i = 0; i < 5_000; i++)
                yield return new StandardRow { Id = i, Payload = "p" };
        }

        await using (var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 500 }))
        {
            await inserter.InitAsync();
            await inserter.AddRangeStreamingAsync(Rows());
            await inserter.CompleteAsync();
        }

        Assert.Equal(5_000UL, await harness.CountAsync());
    }

    [Fact]
    public async Task AddRangeStreamingAsync_IEnumerable_ParityWith_IAsyncEnumerable_OnRowCount()
    {
        // Two harnesses, one fed by IEnumerable, one by IAsyncEnumerable.
        // Both should land the same row count using the same batch size.
        await using var syncHarness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "parity_sync");
        await using var asyncHarness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "parity_async");
        const int rowCount = 2_500;

        await using (var conn = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await conn.OpenAsync();
            await using var inserter = conn.CreateBulkInserter<StandardRow>(syncHarness.TableName,
                new BulkInsertOptions { BatchSize = 250 });
            await inserter.InitAsync();
            await inserter.AddRangeStreamingAsync(SyncRows(rowCount));
            await inserter.CompleteAsync();
        }

        await using (var conn = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await conn.OpenAsync();
            await using var inserter = conn.CreateBulkInserter<StandardRow>(asyncHarness.TableName,
                new BulkInsertOptions { BatchSize = 250 });
            await inserter.InitAsync();
            await inserter.AddRangeStreamingAsync(AsyncRows(rowCount));
            await inserter.CompleteAsync();
        }

        Assert.Equal((ulong)rowCount, await syncHarness.CountAsync());
        Assert.Equal(await syncHarness.CountAsync(), await asyncHarness.CountAsync());

        static IEnumerable<StandardRow> SyncRows(int n)
        {
            for (int i = 0; i < n; i++)
                yield return new StandardRow { Id = i, Payload = "p" };
        }

        static async IAsyncEnumerable<StandardRow> AsyncRows(int n)
        {
            for (int i = 0; i < n; i++)
            {
                await Task.Yield();
                yield return new StandardRow { Id = i, Payload = "p" };
            }
        }
    }

    [Fact]
    public async Task AddRangeStreamingAsync_IEnumerable_GeneratorThrowsMidStream_ExceptionPropagates()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "sync_enum_throw");
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        IEnumerable<StandardRow> ThrowingRows()
        {
            for (int i = 0; i < 1_000; i++)
            {
                if (i == 250) throw new InvalidOperationException("generator failure");
                yield return new StandardRow { Id = i, Payload = "p" };
            }
        }

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 50 });
            await inserter.InitAsync();
            await inserter.AddRangeStreamingAsync(ThrowingRows());
        });
    }
}
