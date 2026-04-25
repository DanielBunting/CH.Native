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
        // Pins single-statement atomicity for a single BulkInsertAsync against a
        // MergeTree table: blocks streamed before the empty terminator are held
        // server-side as uncommitted temp parts (see StreamFailureTests'
        // BulkInsertAsyncEnumerable_NetworkResetMidStream_NothingCommitted for the
        // mid-stream-reset counterpart). So while one INSERT is in flight,
        // concurrent readers must see 0; after CompleteAsync returns and the
        // server commits the temp parts, they must see all 10 000 rows.
        //
        // The previous version of this test asserted maxObservedRows > 0, which
        // contradicts that contract — the only way it could ever succeed is if
        // CH leaked partial state from an uncompleted INSERT. The "Concurrency
        // assertion is hollow" failure was the test correctly diagnosing its
        // own broken premise.
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
            long maxObservedRows = 0L;
            long pollCount = 0L;
            using var stopReaders = new CancellationTokenSource();
            var readersConnected = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var connectedCount = 0;
            var exceptions = new List<Exception>();

            var readerTasks = Enumerable.Range(0, 2).Select(_ => Task.Run(async () =>
            {
                try
                {
                    await using var readConn = new ClickHouseConnection(_fixture.ConnectionString);
                    await readConn.OpenAsync();
                    if (Interlocked.Increment(ref connectedCount) == 2)
                        readersConnected.TrySetResult(true);

                    while (!stopReaders.Token.IsCancellationRequested)
                    {
                        var n = await readConn.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
                        Interlocked.Increment(ref pollCount);
                        long current;
                        do { current = Volatile.Read(ref maxObservedRows); }
                        while (n > current && Interlocked.CompareExchange(ref maxObservedRows, n, current) != current);

                        try { await Task.Delay(5, stopReaders.Token); }
                        catch (OperationCanceledException) { break; }
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    lock (exceptions) exceptions.Add(ex);
                }
            })).ToArray();

            // Don't start the insert until both readers are actually polling — otherwise
            // the assertion-against-0 below could pass trivially because nothing was
            // observed during the in-flight window.
            await readersConnected.Task.WaitAsync(TimeSpan.FromSeconds(10));

            await using var insertConn = new ClickHouseConnection(_fixture.ConnectionString);
            await insertConn.OpenAsync();

            var options = new BulkInsertOptions { BatchSize = 1000 };
            await using var inserter = insertConn.CreateBulkInserter<StressRow>(tableName, options);
            await inserter.InitAsync();

            for (int i = 0; i < 10_000; i++)
            {
                await inserter.AddAsync(new StressRow { Id = i, Name = $"row_{i}" });
            }

            // All 10 Data blocks have been flushed to the server, but the empty
            // terminator hasn't been sent. Hold here until both readers have polled
            // several times in this state — otherwise the assertion is a no-op.
            var pollFloor = Volatile.Read(ref pollCount) + 6;
            var holdDeadline = DateTime.UtcNow.AddSeconds(2);
            while (Volatile.Read(ref pollCount) < pollFloor && DateTime.UtcNow < holdDeadline)
                await Task.Delay(10);

            var maxBeforeComplete = Volatile.Read(ref maxObservedRows);

            await inserter.CompleteAsync();

            // Wait for at least one reader to pick up the post-commit state before
            // snapshotting. Without this, a fast complete + slow reader poll cadence
            // would race with the snapshot.
            var seeAllDeadline = DateTime.UtcNow.AddSeconds(10);
            while (Volatile.Read(ref maxObservedRows) < 10_000 && DateTime.UtcNow < seeAllDeadline)
                await Task.Delay(10);

            var maxAfterComplete = Volatile.Read(ref maxObservedRows);

            stopReaders.Cancel();
            await Task.WhenAll(readerTasks);

            Assert.Empty(exceptions);

            // Mid-INSERT: server holds the 10 000 rows as uncommitted temp parts.
            // Any non-zero observation here would mean CH leaked partial state from
            // an uncompleted INSERT — a server-protocol bug, or a library bug that
            // somehow committed without the terminator.
            Assert.Equal(0L, maxBeforeComplete);

            // Post-CompleteAsync: terminator has been sent + ack'd; temp parts are
            // promoted to committed parts and visible to all readers.
            Assert.Equal(10_000L, maxAfterComplete);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_SingleStatement_NotVisibleBeforeTerminator()
    {
        // Deterministic counterpart to BulkInsert_ConcurrentReaders_During_Insert:
        // no polling, no race windows. Issues a synchronous count() from a second
        // connection between the last AddAsync and CompleteAsync — pins that the
        // 10 Data blocks already on the wire are not visible until the empty
        // terminator commits the statement.
        var tableName = $"test_stress_sync_visibility_{Guid.NewGuid():N}";
        await using var setupConn = new ClickHouseConnection(_fixture.ConnectionString);
        await setupConn.OpenAsync();

        await setupConn.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = MergeTree ORDER BY id");

        try
        {
            await using var readerConn = new ClickHouseConnection(_fixture.ConnectionString);
            await readerConn.OpenAsync();

            await using var insertConn = new ClickHouseConnection(_fixture.ConnectionString);
            await insertConn.OpenAsync();

            var options = new BulkInsertOptions { BatchSize = 1000 };
            await using var inserter = insertConn.CreateBulkInserter<StressRow>(tableName, options);
            await inserter.InitAsync();

            for (int i = 0; i < 10_000; i++)
            {
                await inserter.AddAsync(new StressRow { Id = i, Name = $"row_{i}" });
            }

            // All 10 Data blocks have been flushed to the server. Terminator not sent.
            var midCount = await readerConn.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(0L, midCount);

            await inserter.CompleteAsync();

            var finalCount = await readerConn.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(10_000L, finalCount);
        }
        finally
        {
            await setupConn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_MultipleStatements_ConcurrentReaders_ObservePartialState()
    {
        // Counterpart to BulkInsert_ConcurrentReaders_During_Insert: when callers
        // need partial-commit semantics they must split the work into multiple
        // BulkInsertAsync calls — each one is its own statement and commits
        // independently when its terminator lands. So concurrent readers MUST see
        // an intermediate count strictly between 0 and the total. If they only
        // see {0, total}, the run isn't actually exercising multi-statement
        // visibility (commits collapsed, or readers slept through).
        var tableName = $"test_stress_multi_stmt_read_{Guid.NewGuid():N}";
        const int statements = 10;
        const int rowsPerStatement = 1_000;
        const int totalRows = statements * rowsPerStatement;

        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = MergeTree ORDER BY id");

        try
        {
            long maxObservedRows = 0L;
            var distinctObservations = new HashSet<long>();
            using var stopReaders = new CancellationTokenSource();
            var readersConnected = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var connectedCount = 0;
            var exceptions = new List<Exception>();

            var readerTasks = Enumerable.Range(0, 2).Select(_ => Task.Run(async () =>
            {
                try
                {
                    await using var readConn = new ClickHouseConnection(_fixture.ConnectionString);
                    await readConn.OpenAsync();
                    if (Interlocked.Increment(ref connectedCount) == 2)
                        readersConnected.TrySetResult(true);

                    while (!stopReaders.Token.IsCancellationRequested)
                    {
                        var n = await readConn.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
                        lock (distinctObservations) distinctObservations.Add(n);
                        long current;
                        do { current = Volatile.Read(ref maxObservedRows); }
                        while (n > current && Interlocked.CompareExchange(ref maxObservedRows, n, current) != current);

                        try { await Task.Delay(5, stopReaders.Token); }
                        catch (OperationCanceledException) { break; }
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    lock (exceptions) exceptions.Add(ex);
                }
            })).ToArray();

            await readersConnected.Task.WaitAsync(TimeSpan.FromSeconds(10));

            await using var insertConn = new ClickHouseConnection(_fixture.ConnectionString);
            await insertConn.OpenAsync();

            for (int s = 0; s < statements; s++)
            {
                var startId = s * rowsPerStatement;
                var batch = Enumerable.Range(startId, rowsPerStatement)
                    .Select(i => new StressRow { Id = i, Name = $"row_{i}" });
                await insertConn.BulkInsertAsync(tableName, batch);

                // Brief gap so readers can observe distinct intermediate counts
                // rather than racing all 10 commits before any poll lands.
                await Task.Delay(20);
            }

            var seeAllDeadline = DateTime.UtcNow.AddSeconds(10);
            while (Volatile.Read(ref maxObservedRows) < totalRows && DateTime.UtcNow < seeAllDeadline)
                await Task.Delay(10);

            stopReaders.Cancel();
            await Task.WhenAll(readerTasks);

            Assert.Empty(exceptions);

            int intermediateCount;
            string observedSummary;
            lock (distinctObservations)
            {
                intermediateCount = distinctObservations.Count(c => c > 0 && c < totalRows);
                observedSummary = string.Join(",", distinctObservations.OrderBy(x => x));
            }

            Assert.True(intermediateCount > 0,
                $"Readers never observed an intermediate count strictly between 0 and {totalRows} — " +
                $"either commits collapsed or readers slept through the run. " +
                $"Distinct counts seen: {observedSummary}");

            Assert.Equal((long)totalRows, Volatile.Read(ref maxObservedRows));

            var finalCount = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal((long)totalRows, finalCount);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_MultipleStatements_MonotonicVisibility()
    {
        // Deterministic counterpart to ObservePartialState: no concurrency, no
        // polling. Run N independent BulkInsertAsync calls and read count() from
        // a second connection after each completes. The count must equal the
        // running total at every step — a monotonic, per-statement-visible series.
        // If a future change folds these into one statement (e.g. an internal
        // pooled inserter), this assertion catches the regression.
        var tableName = $"test_stress_multi_stmt_seq_{Guid.NewGuid():N}";
        const int statements = 10;
        const int rowsPerStatement = 1_000;

        await using var setupConn = new ClickHouseConnection(_fixture.ConnectionString);
        await setupConn.OpenAsync();

        await setupConn.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = MergeTree ORDER BY id");

        try
        {
            await using var readerConn = new ClickHouseConnection(_fixture.ConnectionString);
            await readerConn.OpenAsync();

            await using var insertConn = new ClickHouseConnection(_fixture.ConnectionString);
            await insertConn.OpenAsync();

            for (int s = 0; s < statements; s++)
            {
                var startId = s * rowsPerStatement;
                var batch = Enumerable.Range(startId, rowsPerStatement)
                    .Select(i => new StressRow { Id = i, Name = $"row_{i}" });
                await insertConn.BulkInsertAsync(tableName, batch);

                var expected = (long)((s + 1) * rowsPerStatement);
                var observed = await readerConn.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
                Assert.Equal(expected, observed);
            }
        }
        finally
        {
            await setupConn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
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
