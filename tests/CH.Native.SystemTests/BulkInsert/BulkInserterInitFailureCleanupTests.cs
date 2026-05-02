using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pre-fix: when <see cref="BulkInserter{T}.InitAsync"/> failed after
/// <c>SendInsertQueryAsync</c> succeeded but before the schema response
/// completed (e.g. table missing surfaces a server exception mid-handshake),
/// the catch path released the busy slot but did not drain the wire. The
/// connection retained <c>_currentQueryId</c> and the next query on the same
/// connection read the orphaned response, surfacing as a confusing protocol
/// error rather than the original schema error.
///
/// Fix: <c>InitAsync</c>'s generic catch now sends Cancel and drains before
/// re-throwing, so the connection is reusable.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class BulkInserterInitFailureCleanupTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public BulkInserterInitFailureCleanupTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    public class Row
    {
        public int Id { get; set; }
    }

    [Fact]
    public async Task InitAsync_TableMissing_ConnectionStillUsableForNextOperation()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var missingTable = $"definitely_not_a_table_{Guid.NewGuid():N}";

        var inserter = new BulkInserter<Row>(conn, missingTable);
        var initEx = await Assert.ThrowsAnyAsync<Exception>(() => inserter.InitAsync());
        _output.WriteLine($"InitAsync threw: {initEx.GetType().Name}: {initEx.Message}");
        Assert.IsType<ClickHouseServerException>(initEx);
        await inserter.DisposeAsync();

        // Pre-fix the next query would either hang reading the orphaned schema
        // block or surface a protocol-state error. Post-fix the wire is drained
        // and a plain SELECT succeeds.
        var result = await conn.ExecuteScalarAsync<int>("SELECT 42");
        Assert.Equal(42, result);
    }

    [Fact]
    public async Task InitAsync_TableMissing_ConnectionReturnsToPoolHealthy()
    {
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 1,
        });

        var missingTable = $"definitely_not_a_table_{Guid.NewGuid():N}";

        await using (var conn = await ds.OpenConnectionAsync())
        {
            var inserter = new BulkInserter<Row>(conn, missingTable);
            await Assert.ThrowsAnyAsync<ClickHouseServerException>(() => inserter.InitAsync());
            await inserter.DisposeAsync();
        }

        var stats = ds.GetStatistics();
        _output.WriteLine($"Pool after failed init: Total={stats.Total} Idle={stats.Idle}");
        // The connection must remain usable — wire was drained, no protocol
        // damage, so it can return to the pool's idle stack.
        Assert.Equal(1, stats.Total);
        Assert.Equal(1, stats.Idle);

        // Confirm the pooled connection is functional.
        await using (var conn = await ds.OpenConnectionAsync())
        {
            var result = await conn.ExecuteScalarAsync<int>("SELECT 7");
            Assert.Equal(7, result);
        }
    }
}
