using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pins the granularity of <see cref="ClickHouseConnection.InvalidateSchemaCache(string?)"/>:
/// per-table invalidation, all-table invalidation, idempotency, no-op for
/// never-cached tables, and per-DataSource isolation.
/// Companion to <see cref="SchemaCacheStaleAfterAlterTableTests"/> which
/// covers the user workflow.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class InvalidateSchemaCacheGranularityTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public InvalidateSchemaCacheGranularityTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task InvalidateSpecificTable_LeavesOtherEntries_NoArgVariantClearsAll()
    {
        await using var harnessA = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "gran_a");
        await using var harnessB = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "gran_b");
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        await PopulateCache(conn, harnessA.TableName);
        await PopulateCache(conn, harnessB.TableName);
        Assert.Equal(2, conn.SchemaCache.Count);

        conn.InvalidateSchemaCache(harnessA.TableName);
        Assert.Equal(1, conn.SchemaCache.Count);

        conn.InvalidateSchemaCache(null);
        Assert.Equal(0, conn.SchemaCache.Count);
    }

    [Fact]
    public async Task InvalidateSpecificTable_Idempotent_RepeatCallsAreSafe()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "gran_idem");
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        await PopulateCache(conn, harness.TableName);
        Assert.Equal(1, conn.SchemaCache.Count);

        conn.InvalidateSchemaCache(harness.TableName);
        Assert.Equal(0, conn.SchemaCache.Count);

        // Repeat — must not throw, must not corrupt.
        conn.InvalidateSchemaCache(harness.TableName);
        Assert.Equal(0, conn.SchemaCache.Count);
    }

    [Fact]
    public async Task InvalidateNeverCached_IsNoOp_DoesNotThrow()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        Assert.Equal(0, conn.SchemaCache.Count);
        conn.InvalidateSchemaCache("table_that_was_never_cached");
        Assert.Equal(0, conn.SchemaCache.Count);
    }

    [Fact]
    public async Task PerConnectionIsolation_TwoConnections_HaveIndependentCaches()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(), namePrefix: "gran_iso");

        await using var connA = new ClickHouseConnection(_fx.BuildSettings());
        await using var connB = new ClickHouseConnection(_fx.BuildSettings());
        await connA.OpenAsync();
        await connB.OpenAsync();

        await PopulateCache(connA, harness.TableName);
        Assert.Equal(1, connA.SchemaCache.Count);
        // connB's cache is untouched.
        Assert.Equal(0, connB.SchemaCache.Count);

        connA.InvalidateSchemaCache(harness.TableName);
        Assert.Equal(0, connA.SchemaCache.Count);

        // Populating connB after connA's invalidation: caches are still
        // independent.
        await PopulateCache(connB, harness.TableName);
        Assert.Equal(0, connA.SchemaCache.Count);
        Assert.Equal(1, connB.SchemaCache.Count);
    }

    private static async Task PopulateCache(ClickHouseConnection conn, string tableName)
    {
        await using var inserter = conn.CreateBulkInserter<StandardRow>(tableName,
            new BulkInsertOptions { BatchSize = 50, UseSchemaCache = true });
        await inserter.InitAsync();
        await inserter.AddAsync(new StandardRow { Id = 1, Payload = "x" });
        await inserter.CompleteAsync();
    }
}
