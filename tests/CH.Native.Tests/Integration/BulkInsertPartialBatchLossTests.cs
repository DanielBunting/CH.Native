using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Covers the contract around <see cref="BulkInserter{T}.DisposeAsync"/> and unflushed
/// rows. The library prioritises data stability: silently dropping buffered rows is
/// not acceptable, so Dispose is a loud error when called with buffered rows and no
/// explicit <see cref="BulkInserter{T}.CompleteAsync"/>. Callers must call
/// <c>CompleteAsync</c> for persistence — Dispose is teardown, not commit.
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
    public async Task Dispose_WithBufferedRowsAndNoComplete_ThrowsLoudly()
    {
        // Contract: buffered rows at Dispose time without a preceding CompleteAsync
        // is a programming error — rows are not persisted, and the caller deserves
        // a loud signal rather than silent data loss. The underlying connection
        // (or its broken state) is irrelevant: the source-of-truth bug is that
        // CompleteAsync was never called, and that's what the message surfaces.
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

            // Tear the connection down under the inserter — the dispose path will
            // still throw an InvalidOperationException describing the missed complete
            // even though the wire is now broken. Best-effort teardown swallows the
            // wire errors so the caller sees the actionable message.
            await connection.DisposeAsync();

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await inserter.DisposeAsync());
            Assert.Contains("CompleteAsync", ex.Message);
            // Pre-fix the message read "unflushed"; the F2 contract clarification
            // changed it to "un-flushed row(s)" with explicit wording about which
            // rows are LOST vs persisted (see BulkInserter.cs DisposeAsync).
            Assert.Contains("un-flushed", ex.Message);

            // Data did not persist (as expected — CompleteAsync was never called).
            await using var verify = new ClickHouseConnection(_fixture.ConnectionString);
            await verify.OpenAsync();
            var count = await verify.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName}");
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
    public async Task Dispose_WithoutComplete_ThrowsAndLeavesConnectionReusable()
    {
        // Dispose without CompleteAsync + with buffered rows must throw. The best-
        // effort end-of-stream finalisation inside Dispose should leave the
        // underlying connection reusable for subsequent queries on the same
        // connection — data stability doesn't mean the whole connection tears down.
        var tableName = $"test_pbl_noComplete_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32) ENGINE = Memory");

        try
        {
            var inserter = connection.CreateBulkInserter<Row>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new Row { Id = 1 });
            // Intentionally skip CompleteAsync.

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await inserter.DisposeAsync());
            Assert.Contains("CompleteAsync", ex.Message);

            // Connection should still be usable — Dispose finalised protocol state
            // on a best-effort basis before throwing.
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var count = await connection.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName}", cancellationToken: cts.Token);
            Assert.Equal(0, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Dispose_AfterComplete_DoesNotThrow()
    {
        // Happy path: explicit CompleteAsync means Dispose is a no-op and must not
        // throw. This guards against over-zealous changes that would make the
        // `await using` pattern fire-unsafe in the intended-use case.
        var tableName = $"test_pbl_happy_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<Row>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new Row { Id = 1 });
            await inserter.CompleteAsync();
            // implicit DisposeAsync on scope exit — should not throw.
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Dispose_EmptyInserter_DoesNotThrow()
    {
        // Zero buffered rows at dispose time is a clean teardown and must not throw —
        // Dispose still sends the empty end-of-stream block so the connection is
        // reusable, and any failure there propagates (broken-wire is a real concern).
        var tableName = $"test_pbl_empty_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32) ENGINE = Memory");

        try
        {
            await using (var inserter = connection.CreateBulkInserter<Row>(tableName))
            {
                await inserter.InitAsync();
                // No AddAsync calls. Dispose should finalise wire state cleanly.
            }

            var count = await connection.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName}");
            Assert.Equal(0, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private class Row
    {
        public int Id { get; set; }
    }
}
