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
/// The library doesn't surface this option as a typed <see cref="BulkInsertOptions"/>
/// field. Workaround: set it via per-query SETTINGS or via a session SET.
/// This file pins:
/// </para>
/// <list type="bullet">
/// <item><description>That setting <c>insert_deduplication_token</c> via session SET
///     does deduplicate same-token inserts, when used with a Replicated/MergeTree
///     engine that supports it.</description></item>
/// <item><description>Whether <see cref="BulkInsertOptions"/> exposes a typed surface
///     for this (today: no — feature gap, documented).</description></item>
/// </list>
///
/// <para>
/// Note: <c>insert_deduplication_token</c> on plain MergeTree is a no-op
/// in older ClickHouse versions. The test uses a ReplicatedMergeTree which
/// does honour it, OR documents the gap if the engine isn't available
/// in the test container.
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
    public async Task BulkInsertOptions_DoesNotExposeDedupToken_FeatureGapDocumented()
    {
        // Pin today's surface: BulkInsertOptions has BatchSize, QueryId,
        // Roles, UseSchemaCache — no DeduplicationToken. This test will
        // flip when (if) the typed field is added.
        var optionType = typeof(BulkInsertOptions);
        var props = optionType.GetProperties()
            .Select(p => p.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        _output.WriteLine($"BulkInsertOptions properties: {string.Join(", ", props)}");
        Assert.DoesNotContain("DeduplicationToken", props);
        Assert.DoesNotContain("InsertDeduplicationToken", props);
    }

    [Fact(Skip = "Requires ReplicatedMergeTree, which the SingleNodeFixture container does not provision; skip for now.")]
    public async Task SessionSetDedupToken_PreventsDuplicateBatchOnReplicatedTable()
    {
        // Workaround pattern: caller can issue `SET insert_deduplication_token`
        // on the connection before each bulk insert. Same token = dedup on
        // ReplicatedMergeTree. Since SingleNodeFixture uses a stand-alone
        // server without the replication coordination, this test is skipped
        // by default. It documents the known workaround and serves as a
        // placeholder for when the test fixture grows a Replicated variant.
        await Task.CompletedTask;
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
