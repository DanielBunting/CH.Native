using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Cluster;

/// <summary>
/// The positive half of the <c>insert_deduplication_token</c> contract: on a
/// <c>ReplicatedMergeTree</c> engine, bulk-insert batches sharing the same token are
/// deduplicated server-side (so an at-least-once pipeline can retry safely), while
/// batches with different tokens are not. Requires Keeper coordination, so it runs
/// against the cluster fixture rather than the stand-alone SingleNode server.
///
/// <para>
/// The negative case (plain MergeTree ignores the token) and the typed-surface pin
/// live in <c>CH.Native.SystemTests.BulkInsertFailures.InsertDeduplicationTokenTests</c>.
/// </para>
/// </summary>
[Collection("Cluster")]
[Trait(Categories.Name, Categories.Cluster)]
public class InsertDeduplicationTokenReplicatedTests
{
    private readonly ClusterFixture _cluster;
    private readonly ITestOutputHelper _output;

    public InsertDeduplicationTokenReplicatedTests(ClusterFixture cluster, ITestOutputHelper output)
    {
        _cluster = cluster;
        _output = output;
    }

    [Fact]
    public async Task DeduplicationToken_DedupsSameTokenBatch_OnReplicatedMergeTree()
    {
        var table = $"dedup_{Guid.NewGuid():N}";
        await CreateReplicatedAsync(table);
        try
        {
            await using var conn = new ClickHouseConnection(_cluster.BuildSettings(_cluster.Shard1Replica1));
            await conn.OpenAsync();

            var rows = new[]
            {
                new Row { Id = 1, Payload = "a" },
                new Row { Id = 2, Payload = "b" },
                new Row { Id = 3, Payload = "c" },
            };

            var token = $"tok_{Guid.NewGuid():N}";

            // First insert with the token lands all three rows.
            await InsertAsync(conn, table, rows, token);
            Assert.Equal(3UL, await CountAsync(conn, table));

            // Re-inserting with the SAME token is deduplicated — still three rows,
            // even though we sent the batch again.
            await InsertAsync(conn, table, rows, token);
            Assert.Equal(3UL, await CountAsync(conn, table));

            // A DIFFERENT token is a distinct logical batch — it lands. (Using a fresh
            // token also proves dedup keys on the token, not on block content: identical
            // rows are not collapsed when the token differs.)
            await InsertAsync(conn, table, rows, $"tok_{Guid.NewGuid():N}");
            var finalCount = await CountAsync(conn, table);
            _output.WriteLine($"Replicated dedup: final count {finalCount} (expected 6)");
            Assert.Equal(6UL, finalCount);
        }
        finally
        {
            await DropAsync(table);
        }
    }

    [Fact]
    public async Task DeduplicationToken_DropsSameTokenBatch_EvenWhenContentDiffers()
    {
        // The unambiguous proof that the *token* drives dedup, with no block-content
        // confound: ReplicatedMergeTree also content-hashes blocks to dedup identical
        // ones, so re-inserting the same rows could be deduped by content alone. Here
        // the two same-token batches have DIFFERENT rows, so content-hash dedup cannot
        // explain a drop — only the shared token can. (This is exactly the retry-safety
        // contract: a retried batch is dropped purely on its token.)
        var table = $"dedup_{Guid.NewGuid():N}";
        await CreateReplicatedAsync(table);
        try
        {
            await using var conn = new ClickHouseConnection(_cluster.BuildSettings(_cluster.Shard1Replica1));
            await conn.OpenAsync();

            var token = $"tok_{Guid.NewGuid():N}";

            await InsertAsync(conn, table, new[]
            {
                new Row { Id = 1, Payload = "a" },
                new Row { Id = 2, Payload = "b" },
            }, token);
            Assert.Equal(2UL, await CountAsync(conn, table));

            // Different rows, SAME token — must be dropped on the token alone.
            await InsertAsync(conn, table, new[]
            {
                new Row { Id = 100, Payload = "x" },
                new Row { Id = 200, Payload = "y" },
                new Row { Id = 300, Payload = "z" },
            }, token);

            var count = await CountAsync(conn, table);
            _output.WriteLine($"Same-token/different-content dedup: count {count} (expected 2)");
            Assert.Equal(2UL, count);

            // Sanity: the second batch's distinct rows really are absent — confirming the
            // whole batch was dropped, not merged.
            var newRowCount = await conn.ExecuteScalarAsync<ulong>(
                $"SELECT count() FROM {table} WHERE id >= 100");
            Assert.Equal(0UL, newRowCount);
        }
        finally
        {
            await DropAsync(table);
        }
    }

    private static async Task InsertAsync(ClickHouseConnection conn, string table, IReadOnlyList<Row> rows, string token)
    {
        await using var inserter = conn.CreateBulkInserter<Row>(table,
            new BulkInsertOptions { DeduplicationToken = token });
        await inserter.InitAsync();
        await inserter.AddRangeAsync(rows);
        await inserter.CompleteAsync();
    }

    private static async Task<ulong> CountAsync(ClickHouseConnection conn, string table) =>
        await conn.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");

    private async Task CreateReplicatedAsync(string table)
    {
        await using var conn = new ClickHouseConnection(_cluster.BuildSettings(_cluster.Shard1Replica1));
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} ON CLUSTER {ClusterFixture.ClusterName} " +
            $"(id Int64, payload String) " +
            $"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{table}', '{{replica}}') " +
            $"ORDER BY id");
    }

    private async Task DropAsync(string table)
    {
        try
        {
            await using var conn = new ClickHouseConnection(_cluster.BuildSettings(_cluster.Shard1Replica1));
            await conn.OpenAsync();
            await conn.ExecuteNonQueryAsync(
                $"DROP TABLE IF EXISTS {table} ON CLUSTER {ClusterFixture.ClusterName} SYNC");
        }
        catch
        {
            // Best-effort teardown — the unique table name keeps tests isolated regardless.
        }
    }

    private sealed class Row
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public long Id { get; set; }
        [ClickHouseColumn(Name = "payload", Order = 1)] public string Payload { get; set; } = "";
    }
}
