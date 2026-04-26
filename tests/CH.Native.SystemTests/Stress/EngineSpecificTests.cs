using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Stress;

[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class EngineSpecificTests
{
    private readonly SingleNodeFixture _fixture;

    public EngineSpecificTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task BulkInsert_MergeTree_WithOrderBy()
    {
        var tableName = $"test_engine_mt_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = MergeTree ORDER BY id");

        try
        {
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new SimpleRow { Id = 5, Name = "five" });
            await inserter.AddAsync(new SimpleRow { Id = 3, Name = "three" });
            await inserter.AddAsync(new SimpleRow { Id = 1, Name = "one" });
            await inserter.AddAsync(new SimpleRow { Id = 4, Name = "four" });
            await inserter.AddAsync(new SimpleRow { Id = 2, Name = "two" });

            await inserter.CompleteAsync();

            var names = new List<string>();
            await using var reader = await connection.ExecuteReaderAsync(
                $"SELECT name FROM {tableName} ORDER BY id");
            while (await reader.ReadAsync())
            {
                names.Add(reader.GetFieldValue<string>(0));
            }

            Assert.Equal(new[] { "one", "two", "three", "four", "five" }, names);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_ReplacingMergeTree_Dedup()
    {
        var tableName = $"test_engine_rmt_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = ReplacingMergeTree ORDER BY id");

        try
        {
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new SimpleRow { Id = 1, Name = "first_1" });
            await inserter.AddAsync(new SimpleRow { Id = 2, Name = "first_2" });
            await inserter.AddAsync(new SimpleRow { Id = 3, Name = "first_3" });
            await inserter.AddAsync(new SimpleRow { Id = 2, Name = "second_2" });
            await inserter.AddAsync(new SimpleRow { Id = 3, Name = "second_3" });

            await inserter.CompleteAsync();

            await connection.ExecuteNonQueryAsync($"OPTIMIZE TABLE {tableName} FINAL");

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(3, count);

            // Verify the LATEST inserted version won the dedup, not the first.
            // ReplacingMergeTree keeps the last-inserted row per ORDER BY key.
            var name2 = await connection.ExecuteScalarAsync<string>(
                $"SELECT name FROM {tableName} WHERE id = 2");
            var name3 = await connection.ExecuteScalarAsync<string>(
                $"SELECT name FROM {tableName} WHERE id = 3");
            Assert.Equal("second_2", name2);
            Assert.Equal("second_3", name3);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_SummingMergeTree_Aggregation()
    {
        var tableName = $"test_engine_smt_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                key String,
                value Int64
            ) ENGINE = SummingMergeTree(value) ORDER BY key");

        try
        {
            await using var inserter = connection.CreateBulkInserter<SummingRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new SummingRow { Key = "alpha", Value = 10 });
            await inserter.AddAsync(new SummingRow { Key = "alpha", Value = 20 });
            await inserter.AddAsync(new SummingRow { Key = "alpha", Value = 30 });
            await inserter.AddAsync(new SummingRow { Key = "beta", Value = 100 });
            await inserter.AddAsync(new SummingRow { Key = "beta", Value = 200 });

            await inserter.CompleteAsync();

            await connection.ExecuteNonQueryAsync($"OPTIMIZE TABLE {tableName} FINAL");

            var alphaSum = await connection.ExecuteScalarAsync<long>(
                $"SELECT value FROM {tableName} WHERE key = 'alpha'");
            Assert.Equal(60, alphaSum);

            var betaSum = await connection.ExecuteScalarAsync<long>(
                $"SELECT value FROM {tableName} WHERE key = 'beta'");
            Assert.Equal(300, betaSum);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_CollapsingMergeTree_Cancellation()
    {
        var tableName = $"test_engine_cmt_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String,
                sign Int8
            ) ENGINE = CollapsingMergeTree(sign) ORDER BY id");

        try
        {
            await using var inserter = connection.CreateBulkInserter<CollapsingRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new CollapsingRow { Id = 1, Name = "record_1", Sign = 1 });
            await inserter.AddAsync(new CollapsingRow { Id = 1, Name = "record_1", Sign = -1 });

            await inserter.CompleteAsync();

            await connection.ExecuteNonQueryAsync($"OPTIMIZE TABLE {tableName} FINAL");

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(0, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_MergeTree_WithTTL()
    {
        var tableName = $"test_engine_ttl_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String,
                created DateTime
            ) ENGINE = MergeTree ORDER BY id
            TTL created + INTERVAL 1 SECOND");

        try
        {
            var pastTime = DateTime.UtcNow.AddSeconds(-5);

            await using var inserter = connection.CreateBulkInserter<TTLRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new TTLRow { Id = 1, Name = "expiring", Created = pastTime });

            await inserter.CompleteAsync();

            await Task.Delay(TimeSpan.FromSeconds(2));

            // TTL eviction needs a merge to materialise. Loop OPTIMIZE up to 5s so a
            // slow merge cycle on CI doesn't false-fail the test.
            long count = 1;
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
            while (DateTime.UtcNow < deadline)
            {
                await connection.ExecuteNonQueryAsync($"OPTIMIZE TABLE {tableName} FINAL");
                count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
                if (count == 0) break;
                await Task.Delay(200);
            }
            Assert.Equal(0, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_MergeTree_Partitioned()
    {
        var tableName = $"test_engine_part_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String,
                date Date
            ) ENGINE = MergeTree PARTITION BY toYYYYMM(date) ORDER BY id");

        try
        {
            await using var inserter = connection.CreateBulkInserter<PartitionedRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new PartitionedRow { Id = 1, Name = "jan", Date = new DateOnly(2024, 1, 15) });
            await inserter.AddAsync(new PartitionedRow { Id = 2, Name = "jan2", Date = new DateOnly(2024, 1, 20) });
            await inserter.AddAsync(new PartitionedRow { Id = 3, Name = "feb", Date = new DateOnly(2024, 2, 10) });
            await inserter.AddAsync(new PartitionedRow { Id = 4, Name = "feb2", Date = new DateOnly(2024, 2, 25) });
            await inserter.AddAsync(new PartitionedRow { Id = 5, Name = "mar", Date = new DateOnly(2024, 3, 5) });

            await inserter.CompleteAsync();

            var partitions = new List<string>();
            await using var reader = await connection.ExecuteReaderAsync(
                $"SELECT partition FROM system.parts WHERE table = '{tableName}' AND active GROUP BY partition ORDER BY partition");
            while (await reader.ReadAsync())
            {
                partitions.Add(reader.GetFieldValue<string>(0));
            }

            Assert.Equal(3, partitions.Count);
            Assert.Contains("202401", partitions);
            Assert.Contains("202402", partitions);
            Assert.Contains("202403", partitions);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_Memory_Engine_Baseline()
    {
        var tableName = $"test_engine_memory_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName);
            await inserter.InitAsync();

            for (int i = 0; i < 1000; i++)
            {
                await inserter.AddAsync(new SimpleRow { Id = i, Name = $"memory_{i}" });
            }

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1000, count);

            var sampleName = await connection.ExecuteScalarAsync<string>(
                $"SELECT name FROM {tableName} WHERE id = 500");
            Assert.Equal("memory_500", sampleName);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private class SimpleRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "name", Order = 1)]
        public string Name { get; set; } = string.Empty;
    }

    private class SummingRow
    {
        [ClickHouseColumn(Name = "key", Order = 0)]
        public string Key { get; set; } = string.Empty;

        [ClickHouseColumn(Name = "value", Order = 1)]
        public long Value { get; set; }
    }

    private class CollapsingRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "name", Order = 1)]
        public string Name { get; set; } = string.Empty;

        [ClickHouseColumn(Name = "sign", Order = 2)]
        public sbyte Sign { get; set; }
    }

    private class TTLRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "name", Order = 1)]
        public string Name { get; set; } = string.Empty;

        [ClickHouseColumn(Name = "created", Order = 2)]
        public DateTime Created { get; set; }
    }

    private class PartitionedRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "name", Order = 1)]
        public string Name { get; set; } = string.Empty;

        [ClickHouseColumn(Name = "date", Order = 2)]
        public DateOnly Date { get; set; }
    }
}
