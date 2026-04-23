using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Finding #15: <see cref="BulkInserter{T}.DisposeAsync"/> calls CompleteAsync but
/// swallows any exception. Worse, <c>DisposeAsync</c> sets <c>_disposed = true</c>
/// BEFORE calling <c>CompleteAsync</c>, and <c>CompleteAsync</c> guards with
/// <c>ObjectDisposedException.ThrowIf(_disposed, this)</c> — so the implicit complete
/// ALWAYS throws, the exception is always swallowed, and the partial batch is always
/// lost. On top of that, the INSERT query is left half-sent so the connection is left
/// in a desynchronized state that breaks subsequent queries on that connection.
/// </summary>
[Collection("ClickHouse")]
public class BulkInsertPartialBatchLossTests
{
    private readonly ClickHouseFixture _fixture;

    public BulkInsertPartialBatchLossTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Dispose_WithExplicitComplete_PersistsData()
    {
        // Healthy baseline: explicit CompleteAsync + Dispose persists data.
        var tableName = $"test_pbl_{Guid.NewGuid():N}";
        await using var setup = new ClickHouseConnection(_fixture.ConnectionString);
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32) ENGINE = Memory");

        try
        {
            await using (var inserter = setup.CreateBulkInserter<Row>(tableName))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new Row { Id = 1 });
                await inserter.AddAsync(new Row { Id = 2 });
                await inserter.CompleteAsync();
            }

            var count = await setup.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Dispose_WithBrokenConnection_SilentlyDropsPartialBatch()
    {
        // Failure path: if the connection is forcibly closed between AddAsync and
        // DisposeAsync, the implicit CompleteAsync throws — and DisposeAsync swallows.
        // The caller has no way to know their last batch was lost.
        var tableName = $"test_pbl_broken_{Guid.NewGuid():N}";
        await using (var setup = new ClickHouseConnection(_fixture.ConnectionString))
        {
            await setup.OpenAsync();
            await setup.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (Id Int32) ENGINE = Memory");
        }

        try
        {
            var connection = new ClickHouseConnection(_fixture.ConnectionString);
            await connection.OpenAsync();

            var inserter = connection.CreateBulkInserter<Row>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new Row { Id = 100 });
            await inserter.AddAsync(new Row { Id = 101 });

            // Tear the connection down under the inserter. The next wire operation
            // (CompleteAsync -> FlushAsync) will fail.
            await connection.DisposeAsync();

            // Dispose must not throw — current behavior swallows the CompleteAsync error.
            var disposeTask = inserter.DisposeAsync().AsTask();
            var completed = await Task.WhenAny(disposeTask, Task.Delay(TimeSpan.FromSeconds(15)));
            Assert.Same(disposeTask, completed);
            Assert.Null(disposeTask.Exception);

            // Confirm data was lost — the swallow is what makes the loss silent.
            await using var verify = new ClickHouseConnection(_fixture.ConnectionString);
            await verify.OpenAsync();
            var count = await verify.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName}");

            // The inserted rows did NOT make it. This documents the "silent data loss"
            // symptom. If the library is changed to surface or log the error, the
            // assertion on 0 remains correct — only the *silence* changes.
            Assert.Equal(0, count);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Dispose_WithoutComplete_UnexpectedlyBreaksConnection()
    {
        // Regression discovered while exploring #15: calling Dispose without first
        // calling CompleteAsync leaves the underlying connection in a broken state —
        // subsequent queries on the SAME connection hang or fail with "Server closed
        // connection unexpectedly". Expectation: Dispose should either (a) cleanly
        // finish the insert and leave the connection reusable, or (b) reset the
        // connection into a known-unusable state with a fast-failing error.
        var tableName = $"test_pbl_noComplete_{Guid.NewGuid():N}";
        var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32) ENGINE = Memory");

        try
        {
            await using (var inserter = connection.CreateBulkInserter<Row>(tableName))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new Row { Id = 1 });
                // Skip CompleteAsync — rely on DisposeAsync to finish.
            }

            // The connection should remain usable. Cap the check with a short timeout
            // so we do not hang the suite: if the bug is present, this query times out.
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var queryTask = Record.ExceptionAsync(async () =>
                await connection.ExecuteScalarAsync<long>(
                    $"SELECT count() FROM {tableName}", cancellationToken: cts.Token));

            var completed = await Task.WhenAny(queryTask, Task.Delay(TimeSpan.FromSeconds(12)));
            Assert.Same(queryTask, completed);

            var ex = await queryTask;
            Assert.Null(ex);
        }
        finally
        {
            try { await connection.DisposeAsync(); } catch { /* broken */ }
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private class Row
    {
        public int Id { get; set; }
    }
}
