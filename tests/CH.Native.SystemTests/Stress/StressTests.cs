using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Stress;

[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class StressTests
{
    private readonly SingleNodeFixture _fixture;

    public StressTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task BulkInsert_1M_Rows_AllPresent()
    {
        var tableName = $"test_stress_1m_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { BatchSize = 10000 };
            await using var inserter = connection.CreateBulkInserter<StressRow>(tableName, options);
            await inserter.InitAsync();

            for (int i = 0; i < 1_000_000; i++)
            {
                await inserter.AddAsync(new StressRow { Id = i, Name = $"row_{i}" });
            }

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1_000_000, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_1M_Rows_WithCompression()
    {
        var tableName = $"test_stress_1m_lz4_{Guid.NewGuid():N}";
        var connectionString = $"{_fixture.ConnectionString};Compress=true;CompressionMethod=lz4";
        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { BatchSize = 10000 };
            await using var inserter = connection.CreateBulkInserter<StressRow>(tableName, options);
            await inserter.InitAsync();

            for (int i = 0; i < 1_000_000; i++)
            {
                await inserter.AddAsync(new StressRow { Id = i, Name = $"row_{i}" });
            }

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1_000_000, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_ConcurrentReaders_During_Insert()
    {
        var tableName = $"test_stress_concurrent_read_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = MergeTree ORDER BY id");

        try
        {
            using var cts = new CancellationTokenSource();
            var exceptions = new List<Exception>();

            var insertTask = Task.Run(async () =>
            {
                await using var insertConn = new ClickHouseConnection(_fixture.ConnectionString);
                await insertConn.OpenAsync();

                var options = new BulkInsertOptions { BatchSize = 1000 };
                await using var inserter = insertConn.CreateBulkInserter<StressRow>(tableName, options);
                await inserter.InitAsync();

                for (int i = 0; i < 10_000; i++)
                {
                    await inserter.AddAsync(new StressRow { Id = i, Name = $"row_{i}" });
                }

                await inserter.CompleteAsync();
                cts.Cancel();
            });

            var maxObservedRows = 0L;
            var readerTasks = Enumerable.Range(0, 2).Select(_ => Task.Run(async () =>
            {
                try
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        try
                        {
                            await using var readConn = new ClickHouseConnection(_fixture.ConnectionString);
                            await readConn.OpenAsync();
                            var n = await readConn.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
                            // Track max so we can prove readers actually saw in-flight inserts.
                            long current;
                            do { current = Volatile.Read(ref maxObservedRows); }
                            while (n > current && Interlocked.CompareExchange(ref maxObservedRows, n, current) != current);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }

                        await Task.Delay(50);
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    lock (exceptions) exceptions.Add(ex);
                }
            })).ToArray();

            await insertTask;
            await Task.WhenAll(readerTasks);

            Assert.Empty(exceptions);
            // Readers must have seen non-zero rows at some point during the insert run.
            // If maxObservedRows is 0, the test isn't actually observing concurrency —
            // it's measuring two unrelated tasks.
            Assert.True(maxObservedRows > 0,
                $"Readers never observed any inserted rows (maxObservedRows = {maxObservedRows}). " +
                "Concurrency assertion is hollow.");
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_ConcurrentInserters_SameTable()
    {
        var tableName = $"test_stress_concurrent_insert_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = MergeTree ORDER BY id");

        try
        {
            var tasks = Enumerable.Range(0, 4).Select(taskIndex => Task.Run(async () =>
            {
                await using var insertConn = new ClickHouseConnection(_fixture.ConnectionString);
                await insertConn.OpenAsync();

                await using var inserter = insertConn.CreateBulkInserter<StressRow>(tableName);
                await inserter.InitAsync();

                for (int i = 0; i < 2500; i++)
                {
                    int id = taskIndex * 2500 + i;
                    await inserter.AddAsync(new StressRow { Id = id, Name = $"row_{id}" });
                }

                await inserter.CompleteAsync();
            })).ToArray();

            await Task.WhenAll(tasks);

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(10_000, count);

            // Workers wrote disjoint id ranges (taskIndex * 2500 .. taskIndex * 2500 + 2499).
            // Verify min/max + uniqueness as a corruption check — bytes-on-the-wire
            // could be transposed between rows and Count alone wouldn't notice.
            var minId = await connection.ExecuteScalarAsync<int>($"SELECT min(id) FROM {tableName}");
            var maxId = await connection.ExecuteScalarAsync<int>($"SELECT max(id) FROM {tableName}");
            var uniqueIds = await connection.ExecuteScalarAsync<long>($"SELECT uniqExact(id) FROM {tableName}");
            Assert.Equal(0, minId);
            Assert.Equal(9_999, maxId);
            Assert.Equal(10_000L, uniqueIds);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_ConcurrentInserters_DifferentTables()
    {
        var tableNames = Enumerable.Range(0, 4)
            .Select(_ => $"test_stress_diff_{Guid.NewGuid():N}")
            .ToArray();

        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        foreach (var tableName in tableNames)
        {
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE {tableName} (
                    id Int32,
                    name String
                ) ENGINE = Memory");
        }

        try
        {
            var tasks = tableNames.Select((tableName, taskIndex) => Task.Run(async () =>
            {
                await using var insertConn = new ClickHouseConnection(_fixture.ConnectionString);
                await insertConn.OpenAsync();

                await using var inserter = insertConn.CreateBulkInserter<StressRow>(tableName);
                await inserter.InitAsync();

                for (int i = 0; i < 1000; i++)
                {
                    await inserter.AddAsync(new StressRow { Id = i, Name = $"table{taskIndex}_row_{i}" });
                }

                await inserter.CompleteAsync();
            })).ToArray();

            await Task.WhenAll(tasks);

            foreach (var tableName in tableNames)
            {
                var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
                Assert.Equal(1000, count);
            }
        }
        finally
        {
            foreach (var tableName in tableNames)
            {
                await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            }
        }
    }

    [Fact]
    public async Task BulkInsert_RapidSmallBatches()
    {
        var tableName = $"test_stress_rapid_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = MergeTree ORDER BY id");

        try
        {
            for (int i = 0; i < 100; i++)
            {
                await using var insertConn = new ClickHouseConnection(_fixture.ConnectionString);
                await insertConn.OpenAsync();

                await using var inserter = insertConn.CreateBulkInserter<StressRow>(tableName);
                await inserter.InitAsync();

                await inserter.AddAsync(new StressRow { Id = i, Name = $"rapid_{i}" });
                await inserter.CompleteAsync();
            }

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(100, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Query_LargeResultSet_1M_Rows()
    {
        var tableName = $"test_stress_query_1m_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { BatchSize = 10000 };
            await using var inserter = connection.CreateBulkInserter<StressRow>(tableName, options);
            await inserter.InitAsync();

            for (int i = 0; i < 1_000_000; i++)
            {
                await inserter.AddAsync(new StressRow { Id = i, Name = $"row_{i}" });
            }

            await inserter.CompleteAsync();

            long rowCount = 0;
            long idSum = 0;
            await using var reader = await connection.ExecuteReaderAsync($"SELECT id, name FROM {tableName}");
            while (await reader.ReadAsync())
            {
                idSum += reader.GetFieldValue<int>(0);
                rowCount++;
            }

            Assert.Equal(1_000_000, rowCount);
            // Pin column-value correctness: sum of 0..999_999 = 999_999 * 500_000.
            // Without this, a reader returning all-default rows would still pass.
            const long expectedSum = 999_999L * 500_000L / 1L; // arithmetic series; trivially: 499999500000
            Assert.Equal(499999500000L, idSum);
            _ = expectedSum;
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Query_ConcurrentQueries_SameConnectionString()
    {
        var tableName = $"test_stress_concurrent_query_{Guid.NewGuid():N}";
        await using var setupConn = new ClickHouseConnection(_fixture.ConnectionString);
        await setupConn.OpenAsync();

        await setupConn.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = setupConn.CreateBulkInserter<StressRow>(tableName);
            await inserter.InitAsync();

            for (int i = 0; i < 100; i++)
            {
                await inserter.AddAsync(new StressRow { Id = i, Name = $"row_{i}" });
            }

            await inserter.CompleteAsync();

            var tasks = Enumerable.Range(0, 10).Select(_ => Task.Run(async () =>
            {
                await using var queryConn = new ClickHouseConnection(_fixture.ConnectionString);
                await queryConn.OpenAsync();

                var count = await queryConn.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
                return count;
            })).ToArray();

            var results = await Task.WhenAll(tasks);

            foreach (var result in results)
            {
                Assert.Equal(100, result);
            }
        }
        finally
        {
            await setupConn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_AddRangeAsync_Large()
    {
        var tableName = $"test_stress_addrange_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<StressRow>(tableName);
            await inserter.InitAsync();

            var items = Enumerable.Range(0, 50_000)
                .Select(i => new StressRow { Id = i, Name = $"range_{i}" });

            await inserter.AddRangeAsync(items);
            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(50_000, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Connection_RapidOpenClose_100x()
    {
        for (int i = 0; i < 100; i++)
        {
            await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
            await connection.OpenAsync();
            await connection.CloseAsync();
        }

        await using var finalConnection = new ClickHouseConnection(_fixture.ConnectionString);
        await finalConnection.OpenAsync();

        var result = await finalConnection.ExecuteScalarAsync<int>("SELECT 42");
        Assert.Equal(42, result);
    }

    private class StressRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "name", Order = 1)]
        public string Name { get; set; } = string.Empty;
    }
}
