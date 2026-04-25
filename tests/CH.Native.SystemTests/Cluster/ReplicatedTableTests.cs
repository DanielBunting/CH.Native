using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Cluster;

/// <summary>
/// Verifies replicated and distributed table behaviour against a 2x2 cluster.
/// </summary>
[Collection("Cluster")]
[Trait(Categories.Name, Categories.Cluster)]
public class ReplicatedTableTests
{
    private readonly ClusterFixture _cluster;

    public ReplicatedTableTests(ClusterFixture cluster)
    {
        _cluster = cluster;
    }

    [Fact]
    public async Task Insert_ReplicatedMergeTree_ReplicatesAcrossReplicas()
    {
        var table = $"replicated_{Guid.NewGuid():N}";
        await CreateReplicatedAndDistributedAsync(table);

        // Insert into one replica of shard 1.
        await using (var conn = new ClickHouseConnection(_cluster.BuildSettings(_cluster.Shard1Replica1)))
        {
            await conn.OpenAsync();
            await conn.ExecuteNonQueryAsync($"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        }

        // The replicated row should appear on shard 1's other replica within a short window.
        await AssertReplicatedRowCountAsync(_cluster.Shard1Replica2, table, expected: 3);

        // Shard 2 was never written to.
        await AssertReplicatedRowCountAsync(_cluster.Shard2Replica1, table, expected: 0);

        await DropAsync(table);
    }

    [Fact]
    public async Task Distributed_Insert_ShardsByKey()
    {
        var table = $"replicated_{Guid.NewGuid():N}";
        await CreateReplicatedAndDistributedAsync(table);

        await using (var conn = new ClickHouseConnection(_cluster.BuildSettings(_cluster.Shard1Replica1)))
        {
            await conn.OpenAsync();
            // 1000 rows split by id % 2 between the two shards.
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table}_dist SELECT number, toString(number) FROM numbers(1000)");
        }

        // Allow internal_replication-driven async replication to settle.
        await Task.Delay(2000);

        await using var query = new ClickHouseConnection(_cluster.BuildSettings(_cluster.Shard1Replica1));
        await query.OpenAsync();

        var total = await query.ExecuteScalarAsync<ulong>(
            $"SELECT count() FROM {table}_dist");
        Assert.Equal(1000UL, total);

        // Per-shard counts: query each shard's local table directly. Sum must equal total
        // and each shard should hold a meaningful fraction (not all on one shard).
        var shard1Local = await query.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
        await using var shard2Query = new ClickHouseConnection(
            _cluster.BuildSettings(_cluster.Shard2Replica1));
        await shard2Query.OpenAsync();
        var shard2Local = await shard2Query.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");

        Assert.Equal(1000UL, shard1Local + shard2Local);
        // For id-keyed sharding over 1000 rows, both shards should be reasonably balanced.
        Assert.InRange(shard1Local, 300UL, 700UL);
        Assert.InRange(shard2Local, 300UL, 700UL);

        await DropAsync(table);
    }

    [Fact]
    public async Task ReplicaFailover_QueryStillSucceeds_OnSurvivingReplica()
    {
        var table = $"replicated_{Guid.NewGuid():N}";
        await CreateReplicatedAndDistributedAsync(table);

        await using (var conn = new ClickHouseConnection(_cluster.BuildSettings(_cluster.Shard1Replica1)))
        {
            await conn.OpenAsync();
            await conn.ExecuteNonQueryAsync($"INSERT INTO {table} VALUES (42, 'x')");
        }

        await AssertReplicatedRowCountAsync(_cluster.Shard1Replica2, table, expected: 1);

        // Take replica 1 down, query through replica 2.
        await _cluster.StopAsync("chs1r1");
        try
        {
            await using var conn = new ClickHouseConnection(_cluster.BuildSettings(_cluster.Shard1Replica2));
            await conn.OpenAsync();
            var count = await conn.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            Assert.Equal(1UL, count);
        }
        finally
        {
            await _cluster.StartAsync("chs1r1");
        }

        await DropAsync(table);
    }

    private async Task CreateReplicatedAndDistributedAsync(string table)
    {
        // Issue ON CLUSTER from any node so all shards/replicas pick up the DDL.
        await using var conn = new ClickHouseConnection(_cluster.BuildSettings(_cluster.Shard1Replica1));
        await conn.OpenAsync();

        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} ON CLUSTER {ClusterFixture.ClusterName} " +
            $"(id UInt64, value String) " +
            $"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{table}', '{{replica}}') " +
            $"ORDER BY id");

        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table}_dist ON CLUSTER {ClusterFixture.ClusterName} " +
            $"(id UInt64, value String) " +
            $"ENGINE = Distributed({ClusterFixture.ClusterName}, default, {table}, id)");
    }

    private async Task DropAsync(string table)
    {
        await using var conn = new ClickHouseConnection(_cluster.BuildSettings(_cluster.Shard1Replica1));
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"DROP TABLE IF EXISTS {table}_dist ON CLUSTER {ClusterFixture.ClusterName} SYNC");
        await conn.ExecuteNonQueryAsync(
            $"DROP TABLE IF EXISTS {table} ON CLUSTER {ClusterFixture.ClusterName} SYNC");
    }

    private static async Task AssertReplicatedRowCountAsync(NodeEndpoint endpoint, string table, ulong expected)
    {
        // Replication is asynchronous. Poll for up to ~5 s.
        for (int i = 0; i < 25; i++)
        {
            await using var conn = new ClickHouseConnection(
                ClickHouseConnectionSettings.CreateBuilder()
                    .WithHost(endpoint.Host)
                    .WithPort(endpoint.Port)
                    .WithCredentials(ClusterFixture.Username, ClusterFixture.Password)
                    .Build());
            await conn.OpenAsync();
            var count = await conn.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            if (count == expected) return;
            await Task.Delay(200);
        }
        throw new Xunit.Sdk.XunitException(
            $"Replicated row count on {endpoint}/{table} did not reach {expected}.");
    }
}
