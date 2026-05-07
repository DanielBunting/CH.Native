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

    [Fact]
    public async Task AddAsync_BeforeInit_Throws()
    {
        var conn = NewUnopenedConnection();
        await using var inserter = new DynamicBulkInserter(conn, "t", new[] { "id" });
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await inserter.AddAsync(new object?[] { 1 }));
    }

    [Fact]
    public async Task FlushAsync_BeforeInit_Throws()
    {
        var conn = NewUnopenedConnection();
        await using var inserter = new DynamicBulkInserter(conn, "t", new[] { "id" });
        await Assert.ThrowsAsync<InvalidOperationException>(() => inserter.FlushAsync());
    }

    [Fact]
    public async Task CompleteAsync_BeforeInit_Throws()
    {
        var conn = NewUnopenedConnection();
        await using var inserter = new DynamicBulkInserter(conn, "t", new[] { "id" });
        await Assert.ThrowsAsync<InvalidOperationException>(() => inserter.CompleteAsync());
    }
}
