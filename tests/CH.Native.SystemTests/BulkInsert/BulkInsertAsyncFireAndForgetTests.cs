using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pins the convenience wrapper <c>connection.BulkInsertAsync&lt;T&gt;</c>
/// against parity with the explicit Init/Add/Complete lifecycle.
/// Use-cases §5.3 documents this as the recommended fixed-list path.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class BulkInsertAsyncFireAndForgetTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public BulkInsertAsyncFireAndForgetTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task BulkInsertAsync_HappyPath_ProducesSameRowCount_AsExplicitLifecycle()
    {
        await using var convenienceHarness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "bia_conv");
        await using var explicitHarness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "bia_expl");

        var rows = Enumerable.Range(0, 1_000)
            .Select(i => new StandardRow { Id = i, Payload = "p" })
            .ToList();

        // Convenience path
        await using (var conn = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await conn.OpenAsync();
            await conn.BulkInsertAsync(convenienceHarness.TableName, rows);
        }

        // Explicit lifecycle path
        await using (var conn = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await conn.OpenAsync();
            await using var inserter = conn.CreateBulkInserter<StandardRow>(explicitHarness.TableName);
            await inserter.InitAsync();
            foreach (var row in rows)
                await inserter.AddAsync(row);
            await inserter.CompleteAsync();
        }

        Assert.Equal(1_000UL, await convenienceHarness.CountAsync());
        Assert.Equal(await convenienceHarness.CountAsync(), await explicitHarness.CountAsync());
    }

    [Fact]
    public async Task BulkInsertAsync_CancellationMidFlight_RethrowsOperationCanceledException()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "bia_cancel");
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        using var cts = new CancellationTokenSource();

        async IAsyncEnumerable<StandardRow> Rows()
        {
            for (int i = 0; i < 50_000; i++)
            {
                if (i == 100) cts.Cancel();
                await Task.Yield();
                yield return new StandardRow { Id = i, Payload = "p" };
            }
        }

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            conn.BulkInsertAsync(harness.TableName, Rows(),
                new BulkInsertOptions { BatchSize = 50 },
                cts.Token));
    }

    [Fact]
    public async Task BulkInsertAsync_AgainstNonExistentTable_SurfacesServerError()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var bogus = $"never_existed_{Guid.NewGuid():N}";
        var rows = new[] { new StandardRow { Id = 1, Payload = "x" } };

        var ex = await Assert.ThrowsAnyAsync<Exception>(() =>
            conn.BulkInsertAsync(bogus, rows));

        _output.WriteLine($"BulkInsertAsync error: {ex.GetType().Name}: {ex.Message}");
        Assert.IsAssignableFrom<ClickHouseServerException>(ex);
    }

    [Fact]
    public async Task BulkInsertAsync_FromDataSourceRent_ReturnsConnection_ToPool()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "bia_ds");
        await using var ds = new ClickHouseDataSource(_fx.BuildSettings());

        await using (var conn = await ds.OpenConnectionAsync())
        {
            await conn.BulkInsertAsync(harness.TableName, new[]
            {
                new StandardRow { Id = 1, Payload = "x" },
                new StandardRow { Id = 2, Payload = "y" },
            });
        }

        // The bulk insert holds the connection's busy slot for its
        // entire lifecycle. After it completes and the connection
        // disposes, the return-to-pool hook fires. The DataSource
        // accounts for the rent — TotalRentsServed must reflect it
        // regardless of whether the connection ends up returned or
        // discarded.
        var stats = ds.GetStatistics();
        _output.WriteLine($"After fire-and-forget: {stats}");
        Assert.Equal(0, stats.Busy);
        Assert.True(stats.TotalRentsServed >= 1);
        Assert.Equal(2UL, await harness.CountAsync());
    }
}
