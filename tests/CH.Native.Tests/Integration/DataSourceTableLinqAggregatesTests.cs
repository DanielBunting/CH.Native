using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Pins the data-source-bound LINQ execution path. <see cref="ClickHouseDataSource.Table{T}()"/>
/// returns a queryable whose context has no fixed connection — every aggregate
/// operator and every enumeration must rent a connection from the pool, run the
/// query, and return the connection on completion. The
/// <c>AsyncQueryableExtensions</c> aggregates were rewired through
/// <c>ClickHouseQueryContext.AcquireConnectionAsync</c> when the data-source
/// support landed; these tests pin every rewired site so a regression in one
/// won't slip through.
/// </summary>
[Collection("ClickHouse")]
public class DataSourceTableLinqAggregatesTests
{
    private readonly ClickHouseFixture _fixture;

    public DataSourceTableLinqAggregatesTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ToListAsync_OverDataSource_RoundTripsAndReleasesConnection()
    {
        await using var fx = await TableFixture.CreateAsync(_fixture);
        var rows = await fx.DataSource.Table<MetricRow>(fx.TableName)
            .OrderBy(r => r.Id)
            .ToListAsync();

        Assert.Equal(100, rows.Count);
        Assert.Equal(0, fx.DataSource.GetStatistics().Busy);
    }

    [Fact]
    public async Task FirstAsync_OverDataSource_ReturnsFirstAndReleasesConnection()
    {
        await using var fx = await TableFixture.CreateAsync(_fixture);
        var first = await fx.DataSource.Table<MetricRow>(fx.TableName)
            .OrderBy(r => r.Id)
            .FirstAsync();

        Assert.Equal(0, first.Id);
        Assert.Equal(0, fx.DataSource.GetStatistics().Busy);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_OverDataSource_NoMatch_ReturnsDefault()
    {
        await using var fx = await TableFixture.CreateAsync(_fixture);
        var none = await fx.DataSource.Table<MetricRow>(fx.TableName)
            .Where(r => r.Id < 0)
            .FirstOrDefaultAsync();

        Assert.Null(none);
        Assert.Equal(0, fx.DataSource.GetStatistics().Busy);
    }

    [Fact]
    public async Task SingleAsync_OverDataSource_ReturnsExactlyOne()
    {
        await using var fx = await TableFixture.CreateAsync(_fixture);
        var single = await fx.DataSource.Table<MetricRow>(fx.TableName)
            .Where(r => r.Id == 42)
            .SingleAsync();

        Assert.Equal(42, single.Id);
        Assert.Equal(0, fx.DataSource.GetStatistics().Busy);
    }

    [Fact]
    public async Task CountAsync_OverDataSource_UsesScalarShortCircuit()
    {
        await using var fx = await TableFixture.CreateAsync(_fixture);
        var count = await fx.DataSource.Table<MetricRow>(fx.TableName).CountAsync();

        Assert.Equal(100, count);
        Assert.Equal(0, fx.DataSource.GetStatistics().Busy);
    }

    [Fact]
    public async Task CountAsync_WithPredicate_OverDataSource()
    {
        await using var fx = await TableFixture.CreateAsync(_fixture);
        var even = await fx.DataSource.Table<MetricRow>(fx.TableName).CountAsync(r => r.Id % 2 == 0);

        Assert.Equal(50, even);
        Assert.Equal(0, fx.DataSource.GetStatistics().Busy);
    }

    [Fact]
    public async Task LongCountAsync_OverDataSource()
    {
        await using var fx = await TableFixture.CreateAsync(_fixture);
        var count = await fx.DataSource.Table<MetricRow>(fx.TableName).LongCountAsync();

        Assert.Equal(100L, count);
        Assert.Equal(0, fx.DataSource.GetStatistics().Busy);
    }

    [Fact]
    public async Task AnyAsync_OverDataSource()
    {
        await using var fx = await TableFixture.CreateAsync(_fixture);
        Assert.True(await fx.DataSource.Table<MetricRow>(fx.TableName).AnyAsync());
        Assert.True(await fx.DataSource.Table<MetricRow>(fx.TableName).AnyAsync(r => r.Id == 42));
        Assert.False(await fx.DataSource.Table<MetricRow>(fx.TableName).AnyAsync(r => r.Id < 0));
        Assert.Equal(0, fx.DataSource.GetStatistics().Busy);
    }

    [Fact]
    public async Task AllAsync_OverDataSource()
    {
        await using var fx = await TableFixture.CreateAsync(_fixture);
        Assert.True(await fx.DataSource.Table<MetricRow>(fx.TableName).AllAsync(r => r.Id >= 0));
        Assert.False(await fx.DataSource.Table<MetricRow>(fx.TableName).AllAsync(r => r.Id == 0));
    }

    [Fact]
    public async Task SumAsync_OverDataSource()
    {
        await using var fx = await TableFixture.CreateAsync(_fixture);
        var sum = await fx.DataSource.Table<MetricRow>(fx.TableName).SumAsync(r => r.Id);

        Assert.Equal((100 * 99) / 2, sum); // 0..99
        Assert.Equal(0, fx.DataSource.GetStatistics().Busy);
    }

    [Fact]
    public async Task AverageAsync_Int_OverDataSource()
    {
        await using var fx = await TableFixture.CreateAsync(_fixture);
        var avg = await fx.DataSource.Table<MetricRow>(fx.TableName).AverageAsync(r => r.Id);

        Assert.Equal(49.5, avg, 5);
    }

    [Fact]
    public async Task AverageAsync_Double_OverDataSource()
    {
        await using var fx = await TableFixture.CreateAsync(_fixture);
        var avg = await fx.DataSource.Table<MetricRow>(fx.TableName).AverageAsync(r => r.Value);

        Assert.True(avg > 0.0);
    }

    [Fact]
    public async Task MinMaxAsync_OverDataSource()
    {
        await using var fx = await TableFixture.CreateAsync(_fixture);
        var min = await fx.DataSource.Table<MetricRow>(fx.TableName).MinAsync(r => r.Id);
        var max = await fx.DataSource.Table<MetricRow>(fx.TableName).MaxAsync(r => r.Id);

        Assert.Equal(0, min);
        Assert.Equal(99, max);
    }

    [Fact]
    public async Task ChainedWhereOrderByTake_OverDataSource_ProjectsThroughClonedContext()
    {
        // Pin: chained operators after dataSource.Table<T>() must clone the context
        // through ClickHouseQueryProvider.CreateContextForType, which has to keep the
        // DataSource binding (not silently drop to a connection-less context).
        // Without that, the chained queryable would throw on enumeration.
        await using var fx = await TableFixture.CreateAsync(_fixture);
        var ids = await fx.DataSource.Table<MetricRow>(fx.TableName)
            .Where(r => r.Id >= 50)
            .OrderBy(r => r.Id)
            .Take(5)
            .ToListAsync();

        Assert.Equal(new[] { 50, 51, 52, 53, 54 }, ids.Select(r => r.Id));
        Assert.Equal(0, fx.DataSource.GetStatistics().Busy);
    }

    [Fact]
    public async Task SequentialAggregates_OverDataSource_RentAndReturnEachTime()
    {
        // Pin: each aggregate is its own rent — running ten in a row must net to
        // zero busy connections without leaking.
        await using var fx = await TableFixture.CreateAsync(_fixture);
        for (var i = 0; i < 10; i++)
        {
            _ = await fx.DataSource.Table<MetricRow>(fx.TableName).CountAsync();
        }

        Assert.Equal(0, fx.DataSource.GetStatistics().Busy);
    }

    private sealed class MetricRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "value", Order = 1)] public double Value { get; set; }
    }

    /// <summary>
    /// Per-test fixture: spins up a fresh data source, creates a small table, seeds 100
    /// rows, drops the table on dispose. Each test gets isolation without the
    /// per-test boilerplate of CREATE/DROP repeated inline.
    /// </summary>
    private sealed class TableFixture : IAsyncDisposable
    {
        public ClickHouseDataSource DataSource { get; }
        public string TableName { get; }

        private TableFixture(ClickHouseDataSource ds, string tableName)
        {
            DataSource = ds;
            TableName = tableName;
        }

        public static async Task<TableFixture> CreateAsync(ClickHouseFixture fixture)
        {
            var tableName = $"test_dslinq_{Guid.NewGuid():N}";
            var ds = new ClickHouseDataSource(fixture.ConnectionString);

            await using (var setup = await ds.OpenConnectionAsync())
            {
                await setup.ExecuteNonQueryAsync($"""
                    CREATE TABLE {tableName} (
                        id    Int32,
                        value Float64
                    ) ENGINE = Memory
                    """);

                var rng = new Random(1234);
                var rows = Enumerable.Range(0, 100).Select(i => new MetricRow
                {
                    Id = i,
                    Value = rng.NextDouble() * 100.0
                });
                await setup.BulkInsertAsync(tableName, rows);
            }

            return new TableFixture(ds, tableName);
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await using var conn = await DataSource.OpenConnectionAsync();
                await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {TableName}");
            }
            catch { /* best-effort teardown */ }
            await DataSource.DisposeAsync();
        }
    }
}
