using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.StressTests.Fixtures;
using Xunit;

namespace CH.Native.StressTests;

[Collection("ClickHouse")]
public class StressTests
{
    private readonly ClickHouseFixture _fixture;

    public StressTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    [Trait("Category", "Stress")]
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
    [Trait("Category", "Stress")]
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
    [Trait("Category", "Stress")]
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

            // Task that inserts 10K rows
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

            // Two tasks that query count repeatedly during insert
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
                            await readConn.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
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
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    [Trait("Category", "Stress")]
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
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    [Trait("Category", "Stress")]
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
    [Trait("Category", "Stress")]
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
    [Trait("Category", "Stress")]
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

            // Query all rows and iterate
            long rowCount = 0;
            await using var reader = await connection.ExecuteReaderAsync($"SELECT id, name FROM {tableName}");
            while (await reader.ReadAsync())
            {
                rowCount++;
            }

            Assert.Equal(1_000_000, rowCount);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    [Trait("Category", "Stress")]
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
            // Insert some known data
            await using var inserter = setupConn.CreateBulkInserter<StressRow>(tableName);
            await inserter.InitAsync();

            for (int i = 0; i < 100; i++)
            {
                await inserter.AddAsync(new StressRow { Id = i, Name = $"row_{i}" });
            }

            await inserter.CompleteAsync();

            // Fire 10 queries in parallel, each opens its own connection
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
    [Trait("Category", "Stress")]
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
    [Trait("Category", "Stress")]
    public async Task Connection_RapidOpenClose_100x()
    {
        for (int i = 0; i < 100; i++)
        {
            await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
            await connection.OpenAsync();
            await connection.CloseAsync();
        }

        // Verify the last connection still works for a query
        await using var finalConnection = new ClickHouseConnection(_fixture.ConnectionString);
        await finalConnection.OpenAsync();

        var result = await finalConnection.ExecuteScalarAsync<int>("SELECT 42");
        Assert.Equal(42, result);
    }

    #region Test POCOs

    private class StressRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "name", Order = 1)]
        public string Name { get; set; } = string.Empty;
    }

    #endregion
}
