using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Linq.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Linq;

/// <summary>
/// LINQ-over-pool saturation: 16 concurrent renters from a 4-connection pool, each
/// running a queryable end-to-end. Pins the lifecycle contract that
/// <see cref="ClickHouseQueryable{T}"/> built on a rented connection releases the
/// rent on disposal — so the pool returns cleanly to all-idle even when the rent
/// rate exceeds <c>MaxPoolSize</c>.
/// </summary>
[Collection("LinqFacts")]
[Trait(Categories.Name, Categories.Linq)]
public class LinqPoolSaturationTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _node;
    private readonly LinqFactTableFixture _facts;
    private readonly ITestOutputHelper _output;

    public LinqPoolSaturationTests(SingleNodeFixture node, LinqFactTableFixture facts, ITestOutputHelper output)
    {
        _node = node;
        _facts = facts;
        _output = output;
    }

    public Task InitializeAsync() => _facts.EnsureSeededAsync(_node);
    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task SixteenConcurrentRents_AllComplete_PoolReturnsToIdle()
    {
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _node.BuildSettings(),
            MaxPoolSize = 4,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(30),
        });

        // Oracle: same predicate the per-task queries use.
        var expected = await ExecuteOracleCountAsync(_node.BuildSettings(), _facts.TableName);

        const int concurrency = 16;
        var tasks = Enumerable.Range(0, concurrency).Select(_ => Task.Run(async () =>
        {
            await using var conn = await ds.OpenConnectionAsync();
            var count = await conn.Table<LinqFactRow>(_facts.TableName)
                .Where(x => x.Amount > 100).CountAsync();
            return count;
        })).ToArray();

        var results = await Task.WhenAll(tasks);

        // All 16 saw the same answer — i.e., none returned partial / corrupted data.
        Assert.All(results, r => Assert.Equal(expected, r));

        // Pool returns to fully idle.
        var stats = ds.GetStatistics();
        _output.WriteLine($"Pool stats: Total={stats.Total}, Idle={stats.Idle}, Busy={stats.Busy}, PendingWaits={stats.PendingWaits}");
        Assert.Equal(0, stats.Busy);
        Assert.Equal(0, stats.PendingWaits);
        Assert.Equal(stats.Total, stats.Idle);
    }

    [Fact]
    public async Task CancelHalf_RemainderComplete_PoolStaysClean()
    {
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _node.BuildSettings(),
            MaxPoolSize = 4,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(30),
        });

        var expected = await ExecuteOracleCountAsync(_node.BuildSettings(), _facts.TableName);

        const int concurrency = 16;
        const int cancelCount = 8;

        var ctsList = Enumerable.Range(0, concurrency)
            .Select(i => i < cancelCount ? new CancellationTokenSource() : null)
            .ToArray();

        try
        {
            var tasks = Enumerable.Range(0, concurrency).Select(i => Task.Run(async () =>
            {
                var ct = ctsList[i]?.Token ?? CancellationToken.None;
                await using var conn = await ds.OpenConnectionAsync(ct);
                return await conn.Table<LinqFactRow>(_facts.TableName)
                    .Where(x => x.Amount > 100).CountAsync(ct);
            })).ToArray();

            // Cancel the first 8 immediately. They may already have rented and
            // begun executing; the count of cancelled-vs-completed below is a
            // lower bound on cancellations rather than an exact number.
            foreach (var c in ctsList.Where(c => c is not null))
                c!.Cancel();

            int succeeded = 0;
            int cancelled = 0;
            foreach (var t in tasks)
            {
                try
                {
                    var v = await t;
                    Assert.Equal(expected, v);
                    succeeded++;
                }
                catch (OperationCanceledException)
                {
                    cancelled++;
                }
            }

            _output.WriteLine($"succeeded={succeeded}, cancelled={cancelled}");

            // The 8 not-cancelled tasks must always complete successfully.
            Assert.True(succeeded >= concurrency - cancelCount,
                $"Expected ≥ {concurrency - cancelCount} successes; got {succeeded}.");

            // Give returns time to land if a cancelled task got as far as renting.
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < deadline && ds.GetStatistics().Busy > 0)
                await Task.Delay(50);

            var stats = ds.GetStatistics();
            _output.WriteLine($"Final stats: Total={stats.Total}, Idle={stats.Idle}, Busy={stats.Busy}, PendingWaits={stats.PendingWaits}");
            Assert.Equal(0, stats.Busy);
            Assert.Equal(0, stats.PendingWaits);
        }
        finally
        {
            foreach (var c in ctsList) c?.Dispose();
        }
    }

    private static async Task<int> ExecuteOracleCountAsync(ClickHouseConnectionSettings settings, string table)
    {
        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();
        var raw = await LinqAssertions.ExecuteScalarAsync<long>(
            conn, $"SELECT count() FROM {table} WHERE amount > 100");
        return (int)raw;
    }
}
