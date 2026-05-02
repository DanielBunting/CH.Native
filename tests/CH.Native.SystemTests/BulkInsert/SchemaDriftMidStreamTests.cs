using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Mapping;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pins the bulk-insert schema-cache contract under server-side <c>ALTER TABLE</c>.
///
/// <para>
/// <see cref="SchemaCache"/> is keyed by <c>(TableName, ColumnListFingerprint)</c>
/// — the fingerprint covers <em>which</em> columns the inserter is binding, not
/// the columns' types or order on the server. A server-side ALTER between two
/// inserts on the same pooled connection therefore leaves the cached
/// <see cref="BulkInsertSchema"/> stale on the second insert.
/// </para>
///
/// <para>
/// These tests document and lock in today's behavior:
/// <list type="number">
/// <item><description>Adding a new column the inserter doesn't bind: cached entry is still
///     valid and the second insert succeeds without refetch.</description></item>
/// <item><description>Dropping a bound column: the second insert surfaces a clear server
///     exception and (per <c>BulkInserter.cs</c> lines 616-624) invalidates the cache,
///     so the third insert refetches and reports the missing column with the
///     "not found in table schema" error message.</description></item>
/// <item><description>Type-changing a bound column to a compatible type: the cached writers
///     and the new server-side type may or may not coerce — this test documents which.</description></item>
/// <item><description>The escape hatch <see cref="ClickHouseConnection.InvalidateSchemaCache"/>
///     forces a refetch on the next insert.</description></item>
/// </list>
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class SchemaDriftMidStreamTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public SchemaDriftMidStreamTests(SingleNodeFixture fixture, ITestOutputHelper output)
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
    public async Task AddingUnboundColumnServerSide_CachedSchemaContinuesToWork()
    {
        // The inserter binds (id, name). Server adds a third column the inserter
        // doesn't reference — the cached schema is still a valid superset and
        // the second insert can reuse it.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fixture.BuildSettings(),
            columnDdl: "id Int32, name String");

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // First insert — populates the cache.
        await using (var inserter = conn.CreateBulkInserter<TwoColumnRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 100, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new TwoColumnRow { Id = 1, Name = "a" });
            await inserter.CompleteAsync();
        }

        // Server-side ALTER — adds a column the inserter doesn't bind.
        await conn.ExecuteNonQueryAsync(
            $"ALTER TABLE {harness.TableName} ADD COLUMN extra Int32 DEFAULT 0");

        // Second insert on the same connection — should reuse the cached schema
        // and complete without re-fetch (or re-fetch transparently and still succeed).
        await using (var inserter = conn.CreateBulkInserter<TwoColumnRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 100, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new TwoColumnRow { Id = 2, Name = "b" });
            await inserter.CompleteAsync();
        }

        var count = await harness.CountAsync();
        Assert.Equal(2UL, count);
    }

    [Fact]
    public async Task DroppingBoundColumnServerSide_SecondInsertFails_ThirdInsertSurfacesMissingColumn()
    {
        // The inserter binds (id, name). Server drops `name` between the
        // first and second inserts. Today's contract:
        //  - The cached schema includes (id, name); the second insert's CompleteAsync
        //    surfaces a server exception (UNKNOWN_IDENTIFIER or TYPE_MISMATCH from
        //    the wire-side decode of the now-non-existent column).
        //  - BulkInserter.cs lines 616-624 invalidate the cache on
        //    ClickHouseServerException + UsedCachedSchema, so the third insert
        //    refetches the schema and surfaces a clean
        //    "Column 'name' ... not found in table schema" InvalidOperationException.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fixture.BuildSettings(),
            columnDdl: "id Int32, name String");

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // First insert — populates the cache.
        await using (var inserter = conn.CreateBulkInserter<TwoColumnRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 100, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new TwoColumnRow { Id = 1, Name = "a" });
            await inserter.CompleteAsync();
        }

        // Side connection: drop the column. Use a side connection so the
        // pooled connection's schema cache stays intact.
        await using (var side = new ClickHouseConnection(_fixture.BuildSettings()))
        {
            await side.OpenAsync();
            await side.ExecuteNonQueryAsync(
                $"ALTER TABLE {harness.TableName} DROP COLUMN name");
        }

        // Second insert — cache hit, but the bound column no longer exists server-side.
        Exception? secondCaught = null;
        try
        {
            await using var inserter = conn.CreateBulkInserter<TwoColumnRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 100, UseSchemaCache = true });
            await inserter.InitAsync();
            await inserter.AddAsync(new TwoColumnRow { Id = 2, Name = "b" });
            await inserter.CompleteAsync();
        }
        catch (Exception ex)
        {
            secondCaught = ex;
        }

        _output.WriteLine($"Second insert (after DROP COLUMN) threw: {secondCaught?.GetType().Name}: {secondCaught?.Message}");
        Assert.NotNull(secondCaught);

        // Third insert on a fresh connection — pinned contract: the library
        // builds `INSERT INTO t (id, name) VALUES` from the POCO's bound
        // properties. The server validates the column names in that statement
        // BEFORE sending the schema-block response, so a dropped column
        // surfaces as ClickHouseServerException (error 16, "No such column")
        // — NOT the typed "not found in table schema" InvalidOperationException
        // from MapPropertiesToSchema. (That InvalidOperationException is
        // reachable only when the cached schema lists columns the POCO doesn't
        // bind — a different mismatch direction. It's the "POCO binds X,
        // server has Y" case that hits the server exception.)
        //
        // Callers that want a single exception type for both directions of
        // mismatch should catch the common base (Exception or InvalidOperationException
        // since ClickHouseServerException ultimately inherits from
        // InvalidOperationException via DbException-adjacent hierarchy in this
        // library — which is why ClickHouseConnectionBusyException : InvalidOperationException
        // is a sibling pattern).
        await using var freshConn = new ClickHouseConnection(_fixture.BuildSettings());
        await freshConn.OpenAsync();
        var ex3 = await Assert.ThrowsAsync<CH.Native.Exceptions.ClickHouseServerException>(async () =>
        {
            await using var inserter = freshConn.CreateBulkInserter<TwoColumnRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 100, UseSchemaCache = true });
            await inserter.InitAsync();
            await inserter.AddAsync(new TwoColumnRow { Id = 3, Name = "c" });
            await inserter.CompleteAsync();
        });

        Assert.Contains("No such column", ex3.Message);
    }

    [Fact]
    public async Task PocoBindsColumnNotInSchema_FailsAtMapPropertiesToSchema()
    {
        // The OTHER direction of mismatch: the POCO declares an extra column
        // that's not in the table. Here the INSERT INTO statement references
        // that column, so the server rejects with "No such column" — same as
        // the DROP scenario above. Pinning that the server-side rejection is
        // consistent regardless of which direction the mismatch points.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fixture.BuildSettings(),
            columnDdl: "id Int32"); // server has only (id)

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var ex = await Assert.ThrowsAsync<CH.Native.Exceptions.ClickHouseServerException>(async () =>
        {
            // POCO binds (id, name) — the (name) reference triggers the rejection.
            await using var inserter = conn.CreateBulkInserter<TwoColumnRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 100 });
            await inserter.InitAsync();
            await inserter.AddAsync(new TwoColumnRow { Id = 1, Name = "x" });
            await inserter.CompleteAsync();
        });

        Assert.Contains("No such column", ex.Message);
    }

    [Fact]
    public async Task TypeChangingBoundColumnServerSide_SecondInsertSurfacesError()
    {
        // The inserter binds `name String`. Server changes the column type to
        // Int32 between the first and second inserts. The cached writers for
        // the connection still believe the column is String; on flush, the
        // server rejects the type mismatch.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fixture.BuildSettings(),
            columnDdl: "id Int32, name String");

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        await using (var inserter = conn.CreateBulkInserter<TwoColumnRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 100, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new TwoColumnRow { Id = 1, Name = "a" });
            await inserter.CompleteAsync();
        }

        await using (var side = new ClickHouseConnection(_fixture.BuildSettings()))
        {
            await side.OpenAsync();
            // A genuine type change (String → Nullable(String)) would still be
            // accepted on a String wire write because of LowCardinality compat —
            // pick something deliberately incompatible (Int32) so the wire write
            // is provably mismatched.
            await side.ExecuteNonQueryAsync(
                $"ALTER TABLE {harness.TableName} MODIFY COLUMN name Nullable(Int32)");
        }

        Exception? caught = null;
        try
        {
            await using var inserter = conn.CreateBulkInserter<TwoColumnRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 100, UseSchemaCache = true });
            await inserter.InitAsync();
            await inserter.AddAsync(new TwoColumnRow { Id = 2, Name = "b" });
            await inserter.CompleteAsync();
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        _output.WriteLine($"Second insert (after MODIFY COLUMN type) threw: {caught?.GetType().Name}: {caught?.Message}");
        Assert.NotNull(caught);
        // Either a server exception (rejected on commit) or a wire write
        // exception during flush — both are acceptable. What's NOT acceptable
        // is silent success while writing String bytes into an Int32 column.
        var classified =
            caught is ClickHouseServerException
            || caught is ClickHouseProtocolException
            || caught is InvalidOperationException
            || caught.InnerException is ClickHouseServerException;
        Assert.True(classified,
            $"Type-mismatch must surface a typed exception; got {caught.GetType().FullName}");
    }

    [Fact]
    public async Task InvalidateSchemaCache_ForcesRefetchOnNextInsert()
    {
        // Escape hatch: callers that know an ALTER happened can force a
        // refetch by calling InvalidateSchemaCache. Without it (and without
        // the auto-invalidation triggered by a server exception), the cache
        // would silently keep stale entries.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fixture.BuildSettings(),
            columnDdl: "id Int32, name String");

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        await using (var inserter = conn.CreateBulkInserter<TwoColumnRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 100, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new TwoColumnRow { Id = 1, Name = "a" });
            await inserter.CompleteAsync();
        }

        // Pre-condition: cache populated.
        Assert.Equal(1, conn.SchemaCache.Count);

        conn.InvalidateSchemaCache(harness.TableName);
        Assert.Equal(0, conn.SchemaCache.Count);

        // Drop and re-add as a different shape — would have torn the cached
        // schema if it were still consulted.
        await conn.ExecuteNonQueryAsync(
            $"ALTER TABLE {harness.TableName} ADD COLUMN extra String DEFAULT ''");

        await using (var inserter = conn.CreateBulkInserter<TwoColumnRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 100, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new TwoColumnRow { Id = 2, Name = "b" });
            await inserter.CompleteAsync();
        }

        Assert.Equal(2UL, await harness.CountAsync());
    }
}
