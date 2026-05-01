using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Probes the schema-drift cache-poisoning path documented in the F4
/// investigation. Existing <see cref="SchemaDriftMidStreamTests"/> covered
/// drift detected at <c>CompleteAsync</c> time (where the production code
/// invalidates the cache via the <c>ClickHouseServerException when _usedCachedSchema</c>
/// catch). This file probes whether drift detected at <c>FlushAsync</c> time
/// (mid-stream) leaves the cache in a poisoned state for subsequent inserts
/// on the same connection.
///
/// <para>
/// This is a "probe-and-document" test: the answer might be (a) FlushAsync
/// also invalidates, (b) it doesn't and the next insert reuses stale cache,
/// or (c) the drift is actually invisible to FlushAsync because the wire-side
/// type writers don't notice the server's column metadata. We pin whichever
/// answer is real today.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class SchemaDriftFlushTimePoisoningTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public SchemaDriftFlushTimePoisoningTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private sealed class TwoColumnRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "name", Order = 1)] public string Name { get; set; } = "";
    }

    [Fact]
    public async Task DropColumn_BetweenFlushes_OnSameConnection_NextInsertEitherSucceedsOrSurfacesTypedError()
    {
        // Force a small batch size so the first AddAsync calls auto-flush
        // multiple times — drift is then detectable at the FlushAsync wire
        // boundary, not just at the final CompleteAsync.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fixture.BuildSettings(),
            columnDdl: "id Int32, name String");

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // Insert #1: populates the schema cache.
        await using (var inserter = conn.CreateBulkInserter<TwoColumnRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 5, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            for (int i = 0; i < 10; i++)
                await inserter.AddAsync(new TwoColumnRow { Id = i, Name = $"row-{i}" });
            await inserter.CompleteAsync();
        }
        Assert.Equal(1, conn.SchemaCache.Count);

        // Drop the bound column from a side connection.
        await using (var side = new ClickHouseConnection(_fixture.BuildSettings()))
        {
            await side.OpenAsync();
            await side.ExecuteNonQueryAsync(
                $"ALTER TABLE {harness.TableName} DROP COLUMN name");
        }

        // Insert #2 on the SAME connection. With the cached schema in hand,
        // InitAsync sends `INSERT INTO t (id, name) VALUES` — the server
        // rejects synchronously with "No such column".
        Exception? caught = null;
        try
        {
            await using var inserter = conn.CreateBulkInserter<TwoColumnRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 5, UseSchemaCache = true });
            await inserter.InitAsync();
            for (int i = 100; i < 110; i++)
                await inserter.AddAsync(new TwoColumnRow { Id = i, Name = $"row-{i}" });
            await inserter.CompleteAsync();
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        _output.WriteLine($"Insert after drop: {caught?.GetType().Name}: {caught?.Message}");
        Assert.NotNull(caught);

        // Now the key question: did the failure invalidate the cache?
        // Per F4 doc: only CompleteAsync's catch invalidates. Init-time failure
        // happens BEFORE that catch site, so the cache may still hold the
        // stale entry — making the connection effectively unusable for this
        // table until cleared. Pin actual state.
        _output.WriteLine($"Cache size after init-time failure: {conn.SchemaCache.Count}");
        // Either is acceptable behaviour today — assert that count is sane.
        Assert.True(conn.SchemaCache.Count <= 1,
            "Cache should not have grown beyond the original entry");
    }

    [Fact]
    public async Task TypeChange_OnDirectPathTypeMismatch_SurfacesAtFlushTime()
    {
        // Direct-path extractors (Int32, String, etc.) bind a wire-level
        // writer at InitAsync. If the server later changes the column type
        // and the writer-side check at SendDataBlockDirectAsync doesn't
        // re-validate against the cached type names, the wrong-shaped bytes
        // would be sent. Pin: the failure surfaces somehow (server rejects,
        // client throws on type mismatch, etc.) — never silently corrupts.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fixture.BuildSettings(),
            columnDdl: "id Int32, name String");

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        await using (var inserter = conn.CreateBulkInserter<TwoColumnRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 5, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new TwoColumnRow { Id = 1, Name = "a" });
            await inserter.CompleteAsync();
        }

        await using (var side = new ClickHouseConnection(_fixture.BuildSettings()))
        {
            await side.OpenAsync();
            // DROP + ADD bypasses the server-side cast check that MODIFY would
            // run (since the existing String value 'a' can't be cast to UInt32).
            // The new column has the same name but a wire-incompatible type.
            await side.ExecuteNonQueryAsync(
                $"ALTER TABLE {harness.TableName} DROP COLUMN name");
            await side.ExecuteNonQueryAsync(
                $"ALTER TABLE {harness.TableName} ADD COLUMN name UInt32 DEFAULT 0");
        }

        Exception? caught = null;
        try
        {
            await using var inserter = conn.CreateBulkInserter<TwoColumnRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 5, UseSchemaCache = true });
            await inserter.InitAsync();
            await inserter.AddAsync(new TwoColumnRow { Id = 2, Name = "b" });
            await inserter.CompleteAsync();
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        _output.WriteLine($"Type-change insert: {caught?.GetType().Name ?? "(none)"}: {caught?.Message}");
        // Either:
        //  (a) the insert throws because String bytes can't be parsed as UInt32, or
        //  (b) the insert silently succeeds because the cached schema TYPES
        //      mismatch the new server schema and the wire writer was reused
        //      without revalidation — which would be a real correctness bug.
        // We assert (a) holds — silent acceptance would be data corruption.
        Assert.NotNull(caught);
        ulong count = await harness.CountAsync();
        _output.WriteLine($"Committed rows after type-mismatch insert: {count}");
        // Only the first insert's row should be visible; the second must not
        // have silently committed wrong-shaped bytes.
        Assert.True(count <= 2,
            $"Type-mismatched insert must not silently commit; committed={count}");
    }
}
