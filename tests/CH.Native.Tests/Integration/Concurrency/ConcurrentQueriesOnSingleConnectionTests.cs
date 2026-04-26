using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Results;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration.Concurrency;

/// <summary>
/// Tier 2 integration coverage for the connection-busy gate. Each test opens a
/// single <see cref="ClickHouseConnection"/> against a real ClickHouse server,
/// then issues two operations that race for the wire. The library must surface
/// exactly one <see cref="ClickHouseConnectionBusyException"/> and leave the
/// connection cleanly poolable — never silently corrupt the protocol stream.
/// </summary>
[Collection("ClickHouse")]
public class ConcurrentQueriesOnSingleConnectionTests
{
    private readonly ClickHouseFixture _fixture;

    public ConcurrentQueriesOnSingleConnectionTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private sealed class TwoColumnRow
    {
        public long A { get; set; }
        public string B { get; set; } = string.Empty;
    }

    private async Task<string> SetupTableAsync(ClickHouseConnection connection, int rowCount)
    {
        var tableName = $"test_concurrent_{Guid.NewGuid():N}";
        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                A Int64,
                B String
            ) ENGINE = Memory");

        await using var inserter = connection.CreateBulkInserter<TwoColumnRow>(tableName);
        await inserter.InitAsync();
        for (int i = 0; i < rowCount; i++)
        {
            await inserter.AddAsync(new TwoColumnRow { A = i, B = $"row-{i}" });
        }
        await inserter.CompleteAsync();
        return tableName;
    }

    [Fact]
    public async Task TwoQueries_Parallel_ExactlyOneThrowsBusy()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var tableName = await SetupTableAsync(connection, rowCount: 1000);
        try
        {
            // Both tasks attempt ExecuteScalarAsync on the same connection.
            // Exactly one wins; the other throws ClickHouseConnectionBusyException.
            var t1 = connection.ExecuteScalarAsync<long>($"SELECT sum(A) FROM {tableName}");
            var t2 = connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");

            var results = await Task.WhenAll(
                CaptureOutcome(t1),
                CaptureOutcome(t2));

            Assert.Single(results, r => r.Succeeded);
            Assert.Single(results, r => !r.Succeeded);
            Assert.IsType<ClickHouseConnectionBusyException>(results.First(r => !r.Succeeded).Exception);

            // Connection still poolable — the busy throw doesn't kill the connection.
            var subsequent = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1000, subsequent);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task ReaderAndScalar_Parallel_ExactlyOneThrowsBusy()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var tableName = await SetupTableAsync(connection, rowCount: 200);
        try
        {
            var readerTask = connection.ExecuteReaderAsync($"SELECT A FROM {tableName} ORDER BY A");
            var scalarTask = connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");

            var (readerOutcome, scalarOutcome) = await AwaitBoth(readerTask, scalarTask);

            // Exactly one of the two must have thrown busy.
            Assert.True(readerOutcome.Succeeded ^ scalarOutcome.Succeeded);
            var failed = readerOutcome.Succeeded ? scalarOutcome.Exception : readerOutcome.Exception;
            Assert.IsType<ClickHouseConnectionBusyException>(failed);

            // Drain whichever reader survived so the slot frees naturally.
            if (readerOutcome.Succeeded)
            {
                await using var reader = readerOutcome.Result!;
                var rows = 0;
                while (await reader.ReadAsync()) rows++;
                Assert.Equal(200, rows);
            }

            // Connection is poolable for a follow-up query.
            var followup = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(200, followup);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsertOpen_QueryOnSameConnection_ThrowsBusy()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var tableName = $"test_busy_bulk_{Guid.NewGuid():N}";
        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                A Int64,
                B String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<TwoColumnRow>(tableName);
            await inserter.InitAsync();

            // Bulk insert holds the wire — a parallel query must throw busy.
            var ex = await Assert.ThrowsAsync<ClickHouseConnectionBusyException>(
                () => connection.ExecuteScalarAsync<long>("SELECT 1"));
            Assert.NotNull(ex.InFlightQueryId);

            // Bulk insert continues to work — the throw didn't poison the wire.
            await inserter.AddAsync(new TwoColumnRow { A = 1, B = "alice" });
            await inserter.AddAsync(new TwoColumnRow { A = 2, B = "bob" });
            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task ReaderNotFullyDrained_SecondQuery_ThrowsBusy()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var tableName = await SetupTableAsync(connection, rowCount: 1000);
        try
        {
            await using var reader = await connection.ExecuteReaderAsync($"SELECT A FROM {tableName} ORDER BY A");
            Assert.True(await reader.ReadAsync());

            // Reader is mid-stream — second query throws busy.
            await Assert.ThrowsAsync<ClickHouseConnectionBusyException>(
                () => connection.ExecuteScalarAsync<long>("SELECT 1"));

            // Original reader still works.
            var rows = 1;
            while (await reader.ReadAsync()) rows++;
            Assert.Equal(1000, rows);

            // After natural completion, slot is released — third query succeeds.
            var followup = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1000, followup);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task ReaderFullyDrainedNotDisposed_SecondQuery_Succeeds()
    {
        // Pins the chosen reader contract: natural enumerator completion clears
        // the busy slot, no explicit Dispose required to free it. Disposal
        // is still recommended for IAsyncDisposable correctness.
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var tableName = await SetupTableAsync(connection, rowCount: 50);
        try
        {
            ClickHouseDataReader? reader = await connection.ExecuteReaderAsync($"SELECT A FROM {tableName}");
            try
            {
                var rows = 0;
                while (await reader.ReadAsync()) rows++;
                Assert.Equal(50, rows);

                // Reader naturally completed (saw EndOfStream). Slot should be
                // released even though we haven't called DisposeAsync yet.
                var second = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
                Assert.Equal(50, second);
            }
            finally
            {
                await reader.DisposeAsync();
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task SequentialQueries_HundredIterations_NoBusyState()
    {
        // Baseline: serial usage never trips the gate. Counter-test for any
        // false-positive that the new gate might introduce.
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var tableName = await SetupTableAsync(connection, rowCount: 10);
        try
        {
            for (int i = 0; i < 100; i++)
            {
                var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
                Assert.Equal(10, count);
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private record struct Outcome<TResult>(bool Succeeded, TResult? Result, Exception? Exception);

    private static async Task<Outcome<T>> CaptureOutcome<T>(Task<T> task)
    {
        try
        {
            var result = await task;
            return new Outcome<T>(true, result, null);
        }
        catch (Exception ex)
        {
            return new Outcome<T>(false, default, ex);
        }
    }

    private static async Task<(Outcome<ClickHouseDataReader> Reader, Outcome<long> Scalar)> AwaitBoth(
        Task<ClickHouseDataReader> reader, Task<long> scalar)
    {
        var r = CaptureOutcome(reader);
        var s = CaptureOutcome(scalar);
        await Task.WhenAll(r, s);
        return (await r, await s);
    }
}
