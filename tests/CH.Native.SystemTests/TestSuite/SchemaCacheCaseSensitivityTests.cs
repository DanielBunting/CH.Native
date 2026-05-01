using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.TestSuite;

/// <summary>
/// Probes <c>SchemaCache.InvalidateTable</c>'s case-sensitivity behaviour.
/// The implementation uses <c>StringComparison.Ordinal</c>, which means
/// invalidating "MyTable" doesn't invalidate cached entries for "mytable"
/// or "MYTABLE" — even though ClickHouse table names are case-sensitive
/// at storage but typically referenced consistently in user code, callers
/// hitting the same table via different casings would not invalidate
/// each other.
///
/// <para>
/// Also probes the more common production case: same table, different
/// qualification (database-prefixed vs unqualified).
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class SchemaCacheCaseSensitivityTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public SchemaCacheCaseSensitivityTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task InvalidateTable_CaseMismatch_DoesNotInvalidateCachedEntry()
    {
        // OBSERVE: cache is keyed by exact (Ordinal) table name. Two
        // bulk-inserts using different casings produce two cache entries.
        // Invalidating one does NOT clear the other. Pin this — could
        // surprise callers who use mixed casing.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(),
            namePrefix: "case_test");

        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        // Populate cache with the canonical name.
        await using (var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 50, UseSchemaCache = true }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = "x" });
            await inserter.CompleteAsync();
        }
        Assert.Equal(1, conn.SchemaCache.Count);

        // Invalidate using a different casing.
        var upperName = harness.TableName.ToUpperInvariant();
        conn.InvalidateSchemaCache(upperName);

        _output.WriteLine($"Cache count after invalidate('{upperName}'): {conn.SchemaCache.Count}");
        // Pin: case mismatch leaves the cached entry intact.
        Assert.Equal(1, conn.SchemaCache.Count);

        // Invalidating with the exact name DOES clear it.
        conn.InvalidateSchemaCache(harness.TableName);
        Assert.Equal(0, conn.SchemaCache.Count);
    }

    [Fact]
    public async Task InvalidateAll_ClearsCacheRegardlessOfCasing()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(),
            namePrefix: "case_all");

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

        // Null arg = clear everything.
        conn.InvalidateSchemaCache(null);
        Assert.Equal(0, conn.SchemaCache.Count);
    }
}
