using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Probes ClickHouse's <c>insert_deduplication_token</c> support — a
/// per-batch token that lets the server reject duplicate inserts of the
/// same logical batch (essential for at-least-once delivery pipelines that
/// want to deduplicate retries).
///
/// <para>
/// The library surfaces this as the typed <see cref="BulkInsertOptions.DeduplicationToken"/>
/// field, which flows through the bulk-insert path as the
/// <c>insert_deduplication_token</c> query setting. This file pins:
/// </para>
/// <list type="bullet">
/// <item><description>That <see cref="BulkInsertOptions"/> exposes the typed
///     <see cref="BulkInsertOptions.DeduplicationToken"/> property.</description></item>
/// <item><description>That on plain MergeTree the token is a no-op (both inserts land) —
///     a SingleNode sanity check pinning user expectations.</description></item>
/// </list>
///
/// <para>
/// The positive case — same-token batches deduplicate — requires a
/// <c>ReplicatedMergeTree</c> engine and lives in
/// <c>CH.Native.SystemTests.Cluster.InsertDeduplicationTokenReplicatedTests</c>,
/// which uses the Keeper-backed cluster fixture.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class InsertDeduplicationTokenTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public InsertDeduplicationTokenTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public void BulkInsertOptions_ExposesDeduplicationToken()
    {
        // Pin the typed surface: BulkInsertOptions now exposes a nullable
        // DeduplicationToken (string?) that flows through to the
        // insert_deduplication_token query setting.
        var prop = typeof(BulkInsertOptions).GetProperty(nameof(BulkInsertOptions.DeduplicationToken));

        Assert.NotNull(prop);
        Assert.Equal(typeof(string), prop!.PropertyType);
        Assert.True(prop.CanRead && prop.CanWrite);
        Assert.Null(new BulkInsertOptions().DeduplicationToken); // default is null (no token)
    }

    [Fact]
    public async Task PerQuerySettings_DedupToken_DoesNotDeduplicateOnPlainMergeTree()
    {
        // Sanity check on plain MergeTree: insert_deduplication_token is a
        // no-op. Two inserts with the same token both land. This pins
        // user expectations — anyone relying on dedup needs Replicated.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fx.BuildSettings());

        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        // Two identical inserts via raw SQL with the same dedup token.
        // Note: SETTINGS clause must precede VALUES, not follow it.
        var token = $"tok_{Guid.NewGuid():N}";
        var insertSql = $"INSERT INTO {harness.TableName} " +
                        $"SETTINGS insert_deduplication_token = '{token}' " +
                        $"VALUES (1, 'a')";
        await conn.ExecuteNonQueryAsync(insertSql);
        await conn.ExecuteNonQueryAsync(insertSql);

        var count = await harness.CountAsync();
        _output.WriteLine($"Plain MergeTree dedup check: {count} rows (2 inserts with same token)");
        // Plain MergeTree does NOT dedup — both rows land.
        Assert.Equal(2UL, count);
    }
}
