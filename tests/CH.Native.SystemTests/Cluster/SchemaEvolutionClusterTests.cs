using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Cluster;

/// <summary>
/// Schema evolution at cluster scale: <c>ALTER TABLE ... ON CLUSTER</c> while inserts
/// run against multiple shards. Ensures replication catch-up + cache invalidation
/// don't conspire to silently drop data.
/// </summary>
[Collection("Cluster")]
[Trait(Categories.Name, Categories.Cluster)]
public class SchemaEvolutionClusterTests
{
    private readonly ClusterFixture _cluster;

    public SchemaEvolutionClusterTests(ClusterFixture cluster)
    {
        _cluster = cluster;
    }

    [Fact]
    public async Task AddColumnOnCluster_ReplicatesAndDoesNotCorrupt()
    {
        var table = $"evo_cluster_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_cluster.BuildSettings(_cluster.Shard1Replica1));
        await conn.OpenAsync();

        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} ON CLUSTER {ClusterFixture.ClusterName} " +
            $"(id UInt64, name String) " +
            $"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{table}', '{{replica}}') " +
            "ORDER BY id");

        try
        {
            // Initial inserts.
            await using (var ins = conn.CreateBulkInserter<RowV1>(table))
            {
                await ins.InitAsync();
                for (int i = 0; i < 200; i++)
                    await ins.AddAsync(new RowV1 { Id = (ulong)i, Name = $"v1_{i}" });
                await ins.CompleteAsync();
            }

            // Add a column on the cluster.
            await conn.ExecuteNonQueryAsync(
                $"ALTER TABLE {table} ON CLUSTER {ClusterFixture.ClusterName} ADD COLUMN extra String DEFAULT '-'");

            // Allow DDL to propagate.
            await Task.Delay(2000);

            // Insert with the new schema via the OTHER replica to check cluster-wide state.
            await using var conn2 = new ClickHouseConnection(_cluster.BuildSettings(_cluster.Shard1Replica2));
            await conn2.OpenAsync();

            // POCO matches the new schema; insert should succeed.
            await using (var ins = conn2.CreateBulkInserter<RowV2>(table))
            {
                await ins.InitAsync();
                for (int i = 200; i < 400; i++)
                    await ins.AddAsync(new RowV2 { Id = (ulong)i, Name = $"v2_{i}", Extra = "post" });
                await ins.CompleteAsync();
            }

            // Replication may lag — poll combined count.
            ulong total = 0;
            for (int i = 0; i < 25; i++)
            {
                total = await conn.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
                if (total == 400) break;
                await Task.Delay(200);
            }
            Assert.Equal(400UL, total);

            // Sample a row from each batch to verify column values aren't garbled.
            // Pre-ALTER row (id=50): name='v1_50', extra defaults to '-'.
            var preName = await conn.ExecuteScalarAsync<string>(
                $"SELECT name FROM {table} WHERE id = 50");
            Assert.Equal("v1_50", preName);
            // Post-ALTER row (id=300): name='v2_300', extra='post'.
            var postName = await conn.ExecuteScalarAsync<string>(
                $"SELECT name FROM {table} WHERE id = 300");
            var postExtra = await conn.ExecuteScalarAsync<string>(
                $"SELECT extra FROM {table} WHERE id = 300");
            Assert.Equal("v2_300", postName);
            Assert.Equal("post", postExtra);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync(
                $"DROP TABLE IF EXISTS {table} ON CLUSTER {ClusterFixture.ClusterName} SYNC");
        }
    }

    private class RowV1
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public ulong Id { get; set; }
        [ClickHouseColumn(Name = "name", Order = 1)] public string Name { get; set; } = "";
    }

    private class RowV2
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public ulong Id { get; set; }
        [ClickHouseColumn(Name = "name", Order = 1)] public string Name { get; set; } = "";
        [ClickHouseColumn(Name = "extra", Order = 2)] public string Extra { get; set; } = "";
    }
}
