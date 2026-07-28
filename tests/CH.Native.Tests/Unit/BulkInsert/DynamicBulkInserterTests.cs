using CH.Native.BulkInsert;
using CH.Native.Connection;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

/// <summary>
/// Constructor-level validation tests for <see cref="DynamicBulkInserter"/>.
/// These exercise the input-shape contract before any wire activity, so they
/// run without Docker. State-machine and round-trip behavior is covered in
/// the integration suite.
/// </summary>
public class DynamicBulkInserterTests
{
    private static ClickHouseConnection NewUnopenedConnection()
    {
        // The ctor only stores settings — no socket activity — so this is safe
        // for purely-synchronous constructor-validation tests. The connection
        // is never disposed because nothing was opened.
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Port=9000;Database=test_db");
        return new ClickHouseConnection(settings);
    }

    [Fact]
    public void Ctor_NullConnection_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new DynamicBulkInserter(null!, "t", new[] { "id" }));
    }

    [Fact]
    public void Ctor_NullTableName_Throws()
    {
        var conn = NewUnopenedConnection();
        Assert.Throws<ArgumentNullException>(() =>
            new DynamicBulkInserter(conn, (string)null!, new[] { "id" }));
    }

    [Fact]
    public void Ctor_EmptyTableName_Throws()
    {
        var conn = NewUnopenedConnection();
        Assert.Throws<ArgumentException>(() =>
            new DynamicBulkInserter(conn, "", new[] { "id" }));
    }

    [Fact]
    public void Ctor_NullColumnNames_Throws()
    {
        var conn = NewUnopenedConnection();
        Assert.Throws<ArgumentNullException>(() =>
            new DynamicBulkInserter(conn, "t", (IReadOnlyList<string>)null!));
    }

    [Fact]
    public void Ctor_EmptyColumnNames_Throws()
    {
        var conn = NewUnopenedConnection();
        Assert.Throws<ArgumentException>(() =>
            new DynamicBulkInserter(conn, "t", Array.Empty<string>()));
    }

    [Fact]
    public void Ctor_DuplicateColumnNamesCaseInsensitive_Throws()
    {
        var conn = NewUnopenedConnection();
        var ex = Assert.Throws<ArgumentException>(() =>
            new DynamicBulkInserter(conn, "t", new[] { "id", "Id" }));
        Assert.Contains("Duplicate", ex.Message);
    }

    [Fact]
    public void Ctor_NullColumnNameElement_Throws()
    {
        var conn = NewUnopenedConnection();
        Assert.Throws<ArgumentException>(() =>
            new DynamicBulkInserter(conn, "t", new string[] { "id", null! }));
    }

    [Fact]
    public void Ctor_EmptyColumnNameElement_Throws()
    {
        var conn = NewUnopenedConnection();
        Assert.Throws<ArgumentException>(() =>
            new DynamicBulkInserter(conn, "t", new[] { "id", "" }));
    }

    [Fact]
    public void Ctor_BatchSizeZero_Throws()
    {
        var conn = NewUnopenedConnection();
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DynamicBulkInserter(conn, "t", new[] { "id" }, new BulkInsertOptions { BatchSize = 0 }));
    }

    [Fact]
    public void Ctor_QualifiedTableNameWithMultipleDots_Throws()
    {
        var conn = NewUnopenedConnection();
        Assert.Throws<ArgumentException>(() =>
            new DynamicBulkInserter(conn, "a.b.c", new[] { "id" }));
    }

    [Fact]
    public void Ctor_QualifiedNameAccepted()
    {
        var conn = NewUnopenedConnection();
        // Should not throw — qualified names are valid input.
        var inserter = new DynamicBulkInserter(conn, "db_a.events", new[] { "id" });
        Assert.Equal(0, inserter.BufferedCount);
    }

    [Fact]
    public void Ctor_DatabaseTableOverload_NullDatabase_Throws()
    {
        var conn = NewUnopenedConnection();
        Assert.Throws<ArgumentNullException>(() =>
            new DynamicBulkInserter(conn, null!, "t", new[] { "id" }));
    }

    [Fact]
    public void Ctor_DatabaseTableOverload_EmptyDatabase_Throws()
    {
        var conn = NewUnopenedConnection();
        Assert.Throws<ArgumentException>(() =>
            new DynamicBulkInserter(conn, "", "t", new[] { "id" }));
    }

    [Fact]
    public void BufferedCount_NewInserter_IsZero()
    {
        var conn = NewUnopenedConnection();
        var inserter = new DynamicBulkInserter(conn, "t", new[] { "id" });
        Assert.Equal(0, inserter.BufferedCount);
    }

    // Lazy-initialization contract: the first mutating call initializes the
    // inserter (sends the INSERT query). On an unopened connection that init
    // attempt fails inside SendInsertQueryAsync with "Connection is not open."
    // — proving init fired — while pre-init validation and no-op paths must
    // NOT touch the wire at all.

    [Fact]
    public async Task AddAsync_BeforeInit_LazilyInitializes_ThrowsNotOpen()
    {
        var conn = NewUnopenedConnection();
        await using var inserter = new DynamicBulkInserter(conn, "t", new[] { "id" });
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await inserter.AddAsync(new object?[] { 1 }));
        // "not open" (not "must be initialized") proves the lazy init path ran.
        Assert.Contains("not open", ex.Message);
    }

    [Fact]
    public async Task AddAsync_AfterFailedLazyInit_RetriesInit_NotBusy()
    {
        var conn = NewUnopenedConnection();
        await using var inserter = new DynamicBulkInserter(conn, "t", new[] { "id" });
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await inserter.AddAsync(new object?[] { 1 }));

        // A failed lazy init must release the busy slot and leave the inserter
        // retryable: the second Add re-attempts init and fails the same way —
        // not with ClickHouseConnectionBusyException (slot leak) and not with
        // "must be initialized" (latched dead).
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await inserter.AddAsync(new object?[] { 1 }));
        Assert.Contains("not open", ex.Message);
    }

    [Fact]
    public async Task AddAsync_BeforeInit_CancelledToken_ThrowsPlainOCE()
    {
        var conn = NewUnopenedConnection();
        await using var inserter = new DynamicBulkInserter(conn, "t", new[] { "id" });
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Pre-init, a cancelled token must be a plain throw BEFORE any wire
        // activity. On this unopened connection any wire attempt would surface
        // as InvalidOperationException instead — so seeing OCE proves ordering.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await inserter.AddAsync(new object?[] { 1 }, cts.Token));
    }

    [Fact]
    public async Task FlushAsync_BeforeInit_EmptyBuffer_IsNoOp()
    {
        var conn = NewUnopenedConnection();
        await using var inserter = new DynamicBulkInserter(conn, "t", new[] { "id" });
        // Nothing buffered and never initialized: nothing to flush, no wire
        // activity — must succeed even though the connection was never opened.
        await inserter.FlushAsync();
    }

    [Fact]
    public async Task CompleteAsync_BeforeInit_IsNoOp_AndSubsequentAddThrowsCompleted()
    {
        var conn = NewUnopenedConnection();
        await using var inserter = new DynamicBulkInserter(conn, "t", new[] { "id" });
        // Never initialized, nothing added: complete is a no-op that never
        // contacts the server.
        await inserter.CompleteAsync();

        // But it still latches the completed state, so a late Add fails loudly
        // instead of lazily opening a fresh INSERT.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await inserter.AddAsync(new object?[] { 1 }));
        Assert.Contains("completed", ex.Message);
    }

    [Fact]
    public async Task AddAsync_ArityMismatch_BeforeInit_ThrowsArgumentException()
    {
        var conn = NewUnopenedConnection();
        await using var inserter = new DynamicBulkInserter(conn, "t", new[] { "id" });
        // Row validation runs before lazy init: a malformed row must not
        // trigger any initialization (which would throw "not open" here).
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await inserter.AddAsync(new object?[] { 1, 2 }));
    }
}
