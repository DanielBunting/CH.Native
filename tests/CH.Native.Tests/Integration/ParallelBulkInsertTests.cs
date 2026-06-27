using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Exercises <see cref="ParallelBulkInserter{T}"/> and the
/// <see cref="ClickHouseDataSource"/> entry points end-to-end: that fanning a load
/// out across multiple pooled connections persists every row exactly once, that
/// the one-shot helpers report the right count, that connections return to the
/// pool after success and after cancellation, and that oversizing the fan-out is
/// rejected up front.
/// </summary>
[Collection("ClickHouse")]
public class ParallelBulkInsertTests
{
    private readonly ClickHouseFixture _fixture;

    public ParallelBulkInsertTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    public class NumericRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public long Id { get; set; }

        [ClickHouseColumn(Name = "value", Order = 1)]
        public double Value { get; set; }

        [ClickHouseColumn(Name = "created", Order = 2)]
        public DateTime Created { get; set; }
    }

    private async Task<string> CreateTableAsync(ClickHouseConnection connection)
    {
        var tableName = $"test_parallel_{Guid.NewGuid():N}";
        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int64,
                value Float64,
                created DateTime64(3)
            ) ENGINE = MergeTree() ORDER BY id");
        return tableName;
    }

    private static IEnumerable<NumericRow> Rows(int count)
    {
        var baseTime = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        for (int i = 0; i < count; i++)
            yield return new NumericRow { Id = i, Value = i * 1.5, Created = baseTime.AddSeconds(i) };
    }

    private static async IAsyncEnumerable<NumericRow> RowsAsync(int count)
    {
        foreach (var row in Rows(count))
        {
            await Task.Yield();
            yield return row;
        }
    }

    [Fact]
    public async Task Parallel_RoundTrips_AllRowsExactlyOnce()
    {
        const int rowCount = 200_000;
        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionString);
        await using var setup = new ClickHouseConnection(_fixture.ConnectionString);
        await setup.OpenAsync();
        var tableName = await CreateTableAsync(setup);

        try
        {
            await using (var inserter = await dataSource.CreateParallelBulkInserterAsync<NumericRow>(
                tableName, new ParallelBulkInsertOptions { DegreeOfParallelism = 4, BatchSize = 10_000 }))
            {
                Assert.Equal(4, inserter.DegreeOfParallelism);
                await inserter.AddRangeAsync(Rows(rowCount));
                await inserter.CompleteAsync();
                Assert.Equal(rowCount, inserter.RowsWritten);
            }

            // Count proves nothing was lost or duplicated; the sum is a checksum
            // proving the right rows landed (not just the right number).
            var count = await setup.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            var sum = await setup.ExecuteScalarAsync<long>($"SELECT toInt64(sum(id)) FROM {tableName}");
            Assert.Equal(rowCount, count);
            Assert.Equal((long)(rowCount - 1) * rowCount / 2, sum);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsertAsync_OneShot_ReturnsRowCount()
    {
        const int rowCount = 50_000;
        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionString);
        await using var setup = new ClickHouseConnection(_fixture.ConnectionString);
        await setup.OpenAsync();
        var tableName = await CreateTableAsync(setup);

        try
        {
            var written = await dataSource.BulkInsertAsync(
                tableName, Rows(rowCount),
                new ParallelBulkInsertOptions { DegreeOfParallelism = 3, BatchSize = 7_500 });

            Assert.Equal(rowCount, written);
            var count = await setup.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(rowCount, count);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsertAsync_AsyncSource_RoundTrips()
    {
        const int rowCount = 20_000;
        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionString);
        await using var setup = new ClickHouseConnection(_fixture.ConnectionString);
        await setup.OpenAsync();
        var tableName = await CreateTableAsync(setup);

        try
        {
            var written = await dataSource.BulkInsertAsync(
                tableName, RowsAsync(rowCount),
                new ParallelBulkInsertOptions { DegreeOfParallelism = 2, BatchSize = 4_000 });

            Assert.Equal(rowCount, written);
            var count = await setup.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(rowCount, count);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Cancellation_MidStream_UnwindsAndReturnsConnections()
    {
        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionString);
        await using var setup = new ClickHouseConnection(_fixture.ConnectionString);
        await setup.OpenAsync();
        var tableName = await CreateTableAsync(setup);

        try
        {
            using var cts = new CancellationTokenSource();

            async IAsyncEnumerable<NumericRow> SlowRows()
            {
                var baseTime = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                for (int i = 0; ; i++)
                {
                    if (i == 5_000)
                        cts.Cancel();
                    yield return new NumericRow { Id = i, Value = i, Created = baseTime.AddSeconds(i % 86_400) };
                }
            }

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await dataSource.BulkInsertAsync(
                    tableName, SlowRows(),
                    new ParallelBulkInsertOptions { DegreeOfParallelism = 3, BatchSize = 1_000 },
                    cts.Token));

            // Every worker connection must have been returned to the pool.
            var stats = dataSource.GetStatistics();
            Assert.Equal(0, stats.Busy);

            // And the pool is still usable — a fresh rent works.
            Assert.True(await dataSource.PingAsync());
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    // Finding #1: a CompleteAsync that fails must NOT be silently reported as
    // success on a retry. The success flag must only flip after the workers
    // actually committed.
    [Fact]
    public async Task CompleteAsync_AfterFailure_RetryThrowsRatherThanSilentSuccess()
    {
        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionString);
        await using var setup = new ClickHouseConnection(_fixture.ConnectionString);
        await setup.OpenAsync();
        var tableName = await CreateTableAsync(setup);

        try
        {
            await using var inserter = await dataSource.CreateParallelBulkInserterAsync<NumericRow>(
                tableName, new ParallelBulkInsertOptions { DegreeOfParallelism = 2 });
            await inserter.AddRangeAsync(Rows(50));

            using var cts = new CancellationTokenSource();
            cts.Cancel();

            // First completion fails (cancelled mid-flight).
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => inserter.CompleteAsync(cts.Token));

            // A second completion must surface that the operation already failed —
            // not return as if everything committed.
            await Assert.ThrowsAsync<InvalidOperationException>(() => inserter.CompleteAsync());
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    // Drives a full round-trip and asserts the row count, a sum-of-id checksum
    // (proves the right rows landed, not just the right number), and RowsWritten.
    private async Task AssertParallelRoundTripAsync(int rowCount, int degreeOfParallelism, int batchSize)
    {
        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionString);
        await using var setup = new ClickHouseConnection(_fixture.ConnectionString);
        await setup.OpenAsync();
        var tableName = await CreateTableAsync(setup);

        try
        {
            long written;
            await using (var inserter = await dataSource.CreateParallelBulkInserterAsync<NumericRow>(
                tableName, new ParallelBulkInsertOptions { DegreeOfParallelism = degreeOfParallelism, BatchSize = batchSize }))
            {
                await inserter.AddRangeAsync(Rows(rowCount));
                await inserter.CompleteAsync();
                written = inserter.RowsWritten;
            }

            var count = await setup.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            var sum = await setup.ExecuteScalarAsync<long>($"SELECT toInt64(sum(id)) FROM {tableName}");
            Assert.Equal(rowCount, written);
            Assert.Equal(rowCount, count);
            Assert.Equal((long)(rowCount - 1) * rowCount / 2, sum);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    // Fewer rows than workers (and the empty case): every worker still opens an
    // INSERT and commits an empty terminator block, even the ones that drain zero
    // rows. RowsWritten and the table must reflect the true count.
    [Theory]
    [InlineData(0)]   // empty bulk — all four workers commit an empty INSERT
    [InlineData(1)]   // one row, four workers — three drain nothing
    [InlineData(2)]
    [InlineData(3)]
    public Task Parallel_FewerRowsThanWorkers_AllLand(int rowCount)
        => AssertParallelRoundTripAsync(rowCount, degreeOfParallelism: 4, batchSize: 10_000);

    // Whole load is smaller than one batch: each worker does at most a single
    // partial flush at CompleteAsync, never an auto-flush.
    [Fact]
    public Task Parallel_PartialBatch_SmallerThanBatchSize_AllLand()
        => AssertParallelRoundTripAsync(rowCount: 100, degreeOfParallelism: 4, batchSize: 10_000);

    // Row count is not a multiple of the batch size, so every worker ends with a
    // leftover partial batch that only the final CompleteAsync flushes.
    [Fact]
    public Task Parallel_RowCountNotMultipleOfBatch_AllLand()
        => AssertParallelRoundTripAsync(rowCount: 25_000, degreeOfParallelism: 3, batchSize: 10_000);

    // Degenerate single-pipe fan-out: one worker, exercising the parallel path's
    // channel/worker machinery with no actual parallelism.
    [Fact]
    public Task Parallel_DegreeOfParallelismOne_RoundTrips()
        => AssertParallelRoundTripAsync(rowCount: 50_000, degreeOfParallelism: 1, batchSize: 10_000);

    // The one-shot helper over an empty source returns zero and leaves an empty table.
    [Fact]
    public async Task BulkInsertAsync_EmptySource_ReturnsZero()
    {
        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionString);
        await using var setup = new ClickHouseConnection(_fixture.ConnectionString);
        await setup.OpenAsync();
        var tableName = await CreateTableAsync(setup);

        try
        {
            var written = await dataSource.BulkInsertAsync(
                tableName, Array.Empty<NumericRow>(),
                new ParallelBulkInsertOptions { DegreeOfParallelism = 4 });

            Assert.Equal(0, written);
            Assert.Equal(0, await setup.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}"));
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task DegreeOfParallelism_AboveMaxPoolSize_Throws()
    {
        await using var dataSource = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = ClickHouseConnectionSettings.Parse(_fixture.ConnectionString),
            MaxPoolSize = 2,
        });

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await dataSource.CreateParallelBulkInserterAsync<NumericRow>(
                "any_table", new ParallelBulkInsertOptions { DegreeOfParallelism = 8 }));
    }
}
