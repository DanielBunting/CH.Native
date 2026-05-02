using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pins the user-facing workflow around the documented post-DDL schema-cache
/// trap. The cache key is <c>(table, POCO column list)</c> — so the trap is
/// not "different POCO sees stale cache" (different cache key, no hit) but
/// rather "same POCO + ALTER TABLE produces silent data drift". A future
/// refactor that loosens cache-keying or eagerly re-validates would change
/// the user-visible behaviour these tests pin.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class SchemaCacheStaleAfterAlterTableTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public SchemaCacheStaleAfterAlterTableTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task AlterAddColumn_SamePocoReinserted_NewColumnGetsServerDefault_NoError()
    {
        // The silent half of the trap: same POCO column list → cache HIT
        // → INSERT keeps writing the old column set. The new column does
        // not break the insert (server applies its DEFAULT) but the user
        // never gets data into it.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(),
            namePrefix: "altadd_silent");
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        await using (var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 50, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = "first" });
            await inserter.CompleteAsync();
        }
        Assert.Equal(1, conn.SchemaCache.Count);

        await conn.ExecuteNonQueryAsync(
            $"ALTER TABLE {harness.TableName} ADD COLUMN extra String DEFAULT 'default_value'");

        await using (var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 50, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new StandardRow { Id = 2, Payload = "second" });
            await inserter.CompleteAsync();
        }

        // Both rows landed; the new column was filled with the server-side
        // default for both rows. The user wrote nothing into 'extra' — that
        // is the silent data drift.
        var defaults = await conn.ExecuteScalarAsync<ulong>(
            $"SELECT countIf(extra = 'default_value') FROM {harness.TableName}");
        Assert.Equal(2UL, defaults);
    }

    [Fact]
    public async Task AlterRenameColumn_SamePoco_StaleCache_NextInsertSurfacesServerError()
    {
        // The loud half of the trap surfaces when the cached schema's column
        // names no longer exist on the server. A RENAME COLUMN means the
        // INSERT statement (built from the cached column list) references
        // the now-missing old name and the server rejects.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(),
            namePrefix: "altrename");
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        await using (var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 50, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = "first" });
            await inserter.CompleteAsync();
        }

        await conn.ExecuteNonQueryAsync(
            $"ALTER TABLE {harness.TableName} RENAME COLUMN payload TO body");

        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 50, UseSchemaCache = true });
            await inserter.InitAsync();
            await inserter.AddAsync(new StandardRow { Id = 2, Payload = "second" });
            await inserter.CompleteAsync();
        });

        _output.WriteLine($"Trap surfaced: {ex.GetType().Name}: {ex.Message}");
    }

    [Fact]
    public async Task AlterAddColumn_AfterInvalidate_WiderPocoFlowsThroughToFreshSchema()
    {
        // Recovery story: ALTER ADD + invalidate + use a POCO that maps the
        // new column. The fresh schema fetch sees the new column and the
        // wider POCO writes data into it.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(),
            namePrefix: "altadd_recover");
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        await using (var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 50, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = "x" });
            await inserter.CompleteAsync();
        }

        await conn.ExecuteNonQueryAsync(
            $"ALTER TABLE {harness.TableName} ADD COLUMN extra String DEFAULT ''");

        conn.InvalidateSchemaCache(harness.TableName);

        await using (var inserter = conn.CreateBulkInserter<RowWithExtra>(harness.TableName,
            new BulkInsertOptions { BatchSize = 50, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new RowWithExtra { Id = 2, Payload = "y", Extra = "added" });
            await inserter.CompleteAsync();
        }

        var withData = await conn.ExecuteScalarAsync<ulong>(
            $"SELECT countIf(extra = 'added') FROM {harness.TableName}");
        Assert.Equal(1UL, withData);
    }

    [Fact]
    public async Task UseSchemaCacheFalse_ForcesFreshFetch_NoInvalidationNeeded()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(),
            namePrefix: "altadd_skip");
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        await using (var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 50, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = "x" });
            await inserter.CompleteAsync();
        }

        await conn.ExecuteNonQueryAsync(
            $"ALTER TABLE {harness.TableName} ADD COLUMN extra String DEFAULT ''");

        // No invalidation, but UseSchemaCache=false on the next insert
        // forces a fresh schema fetch. Workflow alternative when callers
        // don't want to track which tables to invalidate.
        await using (var inserter = conn.CreateBulkInserter<RowWithExtra>(harness.TableName,
            new BulkInsertOptions { BatchSize = 50, UseSchemaCache = false }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new RowWithExtra { Id = 2, Payload = "y", Extra = "added" });
            await inserter.CompleteAsync();
        }

        var withData = await conn.ExecuteScalarAsync<ulong>(
            $"SELECT countIf(extra = 'added') FROM {harness.TableName}");
        Assert.Equal(1UL, withData);
    }

    [Fact]
    public async Task InvalidateSchemaCacheTable_LeavesOtherTablesIntact()
    {
        await using var harnessA = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(),
            namePrefix: "iso_a");
        await using var harnessB = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(),
            namePrefix: "iso_b");
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        await using (var inserter = conn.CreateBulkInserter<StandardRow>(harnessA.TableName,
            new BulkInsertOptions { BatchSize = 50, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = "a" });
            await inserter.CompleteAsync();
        }
        await using (var inserter = conn.CreateBulkInserter<StandardRow>(harnessB.TableName,
            new BulkInsertOptions { BatchSize = 50, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = "b" });
            await inserter.CompleteAsync();
        }
        Assert.Equal(2, conn.SchemaCache.Count);

        conn.InvalidateSchemaCache(harnessA.TableName);

        // Other table's cache survives.
        Assert.Equal(1, conn.SchemaCache.Count);
    }

    [Fact]
    public async Task InvalidateSchemaCache_KeyMatchingIsOrdinalCaseSensitive_QuickAffirmation()
    {
        // Lightweight affirmation that the invalidation key normalisation
        // matches the cache key. SchemaCacheCaseSensitivityTests covers the
        // detailed contract; this single-assert smoke probe makes the
        // invariant local to the post-DDL workflow story.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(),
            namePrefix: "alt_case");
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        await using (var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 50, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = "x" });
            await inserter.CompleteAsync();
        }
        Assert.Equal(1, conn.SchemaCache.Count);

        conn.InvalidateSchemaCache(harness.TableName.ToUpperInvariant());
        Assert.Equal(1, conn.SchemaCache.Count);

        conn.InvalidateSchemaCache(harness.TableName);
        Assert.Equal(0, conn.SchemaCache.Count);
    }
}

internal sealed class RowWithExtra
{
    [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
    [ClickHouseColumn(Name = "payload", Order = 1)] public string Payload { get; set; } = "";
    [ClickHouseColumn(Name = "extra", Order = 2)] public string Extra { get; set; } = "";
}
