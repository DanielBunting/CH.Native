using CH.Native.BulkInsert;
using CH.Native.Connection;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

/// <summary>
/// Lazy-initialization contract tests for the typed <see cref="BulkInserter{T}"/>.
/// The first mutating call initializes the inserter (sends the INSERT query); on
/// an unopened connection that init attempt fails inside SendInsertQueryAsync
/// with "Connection is not open." — proving init fired — while pre-init
/// validation and no-op paths must not touch the wire at all. Mirrors the lazy
/// section of <see cref="DynamicBulkInserterTests"/>; round-trip behavior is
/// covered in the system-test suite.
/// </summary>
public class BulkInserterLazyInitTests
{
    private sealed class Row
    {
        public int Id { get; set; }
    }

    private static ClickHouseConnection NewUnopenedConnection()
    {
        // The ctor only stores settings — no socket activity — so this is safe
        // for tests that never reach a live wire. The connection is never
        // disposed because nothing was opened.
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Port=9000;Database=test_db");
        return new ClickHouseConnection(settings);
    }

    [Fact]
    public async Task AddAsync_BeforeInit_LazilyInitializes_ThrowsNotOpen()
    {
        var conn = NewUnopenedConnection();
        await using var inserter = new BulkInserter<Row>(conn, "t");
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await inserter.AddAsync(new Row { Id = 1 }));
        // "not open" (not "must be initialized") proves the lazy init path ran.
        Assert.Contains("not open", ex.Message);
    }

    [Fact]
    public async Task AddAsync_AfterFailedLazyInit_RetriesInit_NotBusy()
    {
        var conn = NewUnopenedConnection();
        await using var inserter = new BulkInserter<Row>(conn, "t");
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await inserter.AddAsync(new Row { Id = 1 }));

        // A failed lazy init must release the busy slot and leave the inserter
        // retryable: the second Add re-attempts init and fails the same way —
        // not with ClickHouseConnectionBusyException (slot leak) and not with
        // "must be initialized" (latched dead).
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await inserter.AddAsync(new Row { Id = 1 }));
        Assert.Contains("not open", ex.Message);
    }

    [Fact]
    public async Task AddAsync_BeforeInit_CancelledToken_ThrowsPlainOCE()
    {
        var conn = NewUnopenedConnection();
        await using var inserter = new BulkInserter<Row>(conn, "t");
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Pre-init, a cancelled token must be a plain throw BEFORE any wire
        // activity. On this unopened connection any wire attempt would surface
        // as InvalidOperationException instead — so seeing OCE proves ordering.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await inserter.AddAsync(new Row { Id = 1 }, cts.Token));
    }

    [Fact]
    public async Task FlushAsync_BeforeInit_EmptyBuffer_IsNoOp()
    {
        var conn = NewUnopenedConnection();
        await using var inserter = new BulkInserter<Row>(conn, "t");
        // Nothing buffered and never initialized: nothing to flush, no wire
        // activity — must succeed even though the connection was never opened.
        await inserter.FlushAsync();
    }

    [Fact]
    public async Task CompleteAsync_BeforeInit_IsNoOp_AndSubsequentAddThrowsCompleted()
    {
        var conn = NewUnopenedConnection();
        await using var inserter = new BulkInserter<Row>(conn, "t");
        // Never initialized, nothing added: complete is a no-op that never
        // contacts the server.
        await inserter.CompleteAsync();

        // But it still latches the completed state, so a late Add fails loudly
        // instead of lazily opening a fresh INSERT.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await inserter.AddAsync(new Row { Id = 1 }));
        Assert.Contains("completed", ex.Message);
    }

    [Fact]
    public async Task InitAsync_Idempotent_SecondCallIsNoOp()
    {
        var conn = NewUnopenedConnection();
        await using var inserter = new BulkInserter<Row>(conn, "t");
        // Both calls fail identically ("not open") — the second must retry
        // initialization rather than throw "already initialized".
        var first = await Assert.ThrowsAsync<InvalidOperationException>(() => inserter.InitAsync());
        var second = await Assert.ThrowsAsync<InvalidOperationException>(() => inserter.InitAsync());
        Assert.Contains("not open", first.Message);
        Assert.Contains("not open", second.Message);
    }
}
