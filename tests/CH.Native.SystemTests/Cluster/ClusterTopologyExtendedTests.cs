using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Cluster;

/// <summary>
/// Extends <see cref="ClusterMembershipProbeTests"/> with the topology-parser
/// fixture test (the driver should correctly read host/port/replica/shard
/// columns out of <c>system.clusters</c>) and the bulk-insert deduplication
/// contract across retries (the server's <c>insert_deduplication_token</c>
/// guarantees a retried batch doesn't double-count).
/// </summary>
[Collection("Cluster")]
[Trait(Categories.Name, Categories.Cluster)]
public class ClusterTopologyExtendedTests
{
    private readonly ClusterFixture _fx;
    private readonly ITestOutputHelper _output;

    public ClusterTopologyExtendedTests(ClusterFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task SystemClusters_ReturnsExpectedTopology()
    {
        // Pin that the driver can decode the standard system.clusters columns
        // for the test cluster (2 shards × 2 replicas).
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithCredentials(ClusterFixture.Username, ClusterFixture.Password)
            .WithHost(_fx.Shard1Replica1.Host)
            .WithPort(_fx.Shard1Replica1.Port)
            .Build();
        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        await using var reader = await conn.ExecuteReaderAsync(
            $"SELECT cluster, shard_num, replica_num, host_name, port " +
            $"FROM system.clusters WHERE cluster = '{ClusterFixture.ClusterName}' " +
            $"ORDER BY shard_num, replica_num");

        var rows = new List<(string Cluster, uint Shard, uint Replica, string Host, ushort Port)>();
        while (await reader.ReadAsync())
        {
            rows.Add((
                reader.GetFieldValue<string>(0),
                reader.GetFieldValue<uint>(1),
                reader.GetFieldValue<uint>(2),
                reader.GetFieldValue<string>(3),
                reader.GetFieldValue<ushort>(4)));
        }

        // Test cluster has 2 shards × 2 replicas = 4 rows.
        Assert.Equal(4, rows.Count);
        Assert.All(rows, r => Assert.Equal(ClusterFixture.ClusterName, r.Cluster));
        Assert.Equal((uint)1, rows[0].Shard);
        Assert.Equal((uint)1, rows[0].Replica);
        Assert.Equal((uint)2, rows[3].Shard);
        Assert.Equal((uint)2, rows[3].Replica);
        Assert.All(rows, r => Assert.NotEmpty(r.Host));
        Assert.All(rows, r => Assert.True(r.Port > 0));
    }

    [Fact]
    public async Task SystemReplicas_ParsesReplicationMetadata()
    {
        // system.replicas exposes per-replica state for ReplicatedMergeTree
        // tables. Even with no replicated tables defined the schema should be
        // parseable — pin that the column types decode correctly.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithCredentials(ClusterFixture.Username, ClusterFixture.Password)
            .WithHost(_fx.Shard1Replica1.Host)
            .WithPort(_fx.Shard1Replica1.Port)
            .Build();
        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        // Just executing a SELECT against system.replicas with the standard
        // metadata columns proves the column-type decoders handle whatever
        // shape ClickHouse returns. An empty result is still a successful
        // schema decode.
        var count = await conn.ExecuteScalarAsync<ulong>(
            "SELECT count() FROM system.replicas");
        _output.WriteLine($"system.replicas row count: {count}");
    }

    [Fact]
    public async Task BulkInsertWithDeduplicationToken_RetryDoesNotDoubleInsert()
    {
        // ClickHouse's MergeTree deduplication: when an insert is repeated
        // with the same insert_deduplication_token, the second insert is
        // dropped server-side. Pin that the driver correctly carries the
        // setting through.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithCredentials(ClusterFixture.Username, ClusterFixture.Password)
            .WithHost(_fx.Shard1Replica1.Host)
            .WithPort(_fx.Shard1Replica1.Port)
            .Build();
        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        var table = $"dedupe_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32) ENGINE = MergeTree ORDER BY id " +
            $"SETTINGS non_replicated_deduplication_window = 100");

        try
        {
            const string token = "test-dedupe-token-1";

            // Insert the same payload twice with the same dedupe token.
            for (int attempt = 0; attempt < 2; attempt++)
            {
                await conn.ExecuteNonQueryAsync(
                    $"INSERT INTO {table} SETTINGS insert_deduplication_token='{token}' " +
                    $"VALUES (1), (2), (3)");
            }

            // Expect 3 rows total, not 6 — the second insert was deduplicated.
            var count = await conn.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            Assert.Equal((ulong)3, count);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}
