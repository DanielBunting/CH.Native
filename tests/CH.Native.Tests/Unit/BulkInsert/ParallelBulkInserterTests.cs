using CH.Native.BulkInsert;
using CH.Native.Connection;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

/// <summary>
/// Server-independent guard tests for <see cref="ParallelBulkInserter{T}"/>: the
/// argument and lifecycle checks that fire before (or without) any worker
/// connection is opened. The end-to-end fan-out behaviour lives in the
/// integration suite.
/// </summary>
public class ParallelBulkInserterTests
{
    private sealed class Row
    {
        public long Id { get; set; }
    }

    // A DataSource is only needed as the inserter's owner; these tests never open
    // a connection, so a parse-only connection string is enough.
    private static ClickHouseDataSource NewDataSource() =>
        new("Host=localhost;Port=9000");

    private static ParallelBulkInserter<Row> NewInserter(ClickHouseDataSource dataSource, string? tableName = "t") =>
        new(dataSource, database: null, tableName!, ParallelBulkInsertOptions.Default, degreeOfParallelism: 2);

    [Fact]
    public void Ctor_NullTableName_Throws()
    {
        using var ds = NewDataSource();
        Assert.Throws<ArgumentNullException>(() => NewInserter(ds, tableName: null));
    }

    [Fact]
    public void DegreeOfParallelism_ReflectsConstructorValue()
    {
        using var ds = NewDataSource();
        var inserter = new ParallelBulkInserter<Row>(ds, database: null, "t", ParallelBulkInsertOptions.Default, degreeOfParallelism: 3);
        Assert.Equal(3, inserter.DegreeOfParallelism);
        Assert.Equal(0, inserter.RowsWritten);
    }

    [Fact]
    public async Task DisposeAsync_BeforeStart_IsCleanAndIdempotent()
    {
        using var ds = NewDataSource();
        var inserter = NewInserter(ds);

        // Never started: no workers to join, just tears down the channel + CTS.
        await inserter.DisposeAsync();
        // A second dispose must short-circuit rather than touch a disposed CTS.
        await inserter.DisposeAsync();
    }

    [Fact]
    public async Task AddAsync_AfterDispose_ThrowsObjectDisposed()
    {
        using var ds = NewDataSource();
        var inserter = NewInserter(ds);
        await inserter.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await inserter.AddAsync(new Row()));
    }

    [Fact]
    public async Task CompleteAsync_AfterDispose_ThrowsObjectDisposed()
    {
        using var ds = NewDataSource();
        var inserter = NewInserter(ds);
        await inserter.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => inserter.CompleteAsync());
    }

    [Fact]
    public async Task AddRangeAsync_NullSource_Throws()
    {
        using var ds = NewDataSource();
        await using var inserter = NewInserter(ds);
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await inserter.AddRangeAsync(null!));
    }

    [Fact]
    public async Task AddRangeStreamingAsync_NullSource_Throws()
    {
        using var ds = NewDataSource();
        await using var inserter = NewInserter(ds);
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await inserter.AddRangeStreamingAsync(null!));
    }

    [Fact]
    public async Task AddAsync_BlockedOnFullChannel_ThenTornDown_Surfaces()
    {
        using var ds = NewDataSource();
        // Capacity 1 so the second push must take the awaiting slow path.
        var options = new ParallelBulkInsertOptions { ChannelCapacity = 1, DegreeOfParallelism = 1 };
        var inserter = new ParallelBulkInserter<Row>(ds, database: null, "t", options, degreeOfParallelism: 1);

        // Fill the only channel slot. No workers are draining, so the next push stalls.
        await inserter.AddAsync(new Row());

        var blocked = Task.Run(async () => await inserter.AddAsync(new Row()));

        // Let the second push get past the guard + TryWrite and settle into the
        // awaiting slow path before we tear the channel down underneath it.
        await Task.Delay(100);

        // Tear the channel down the way an abandoning dispose does. With no recorded
        // worker fault and an un-cancelled caller token, the stalled push observes the
        // completed writer and surfaces a ChannelClosedException rather than hanging.
        await inserter.DisposeAsync();

        await Assert.ThrowsAsync<System.Threading.Channels.ChannelClosedException>(async () => await blocked);
    }

    [Fact]
    public void Options_Default_IsSharedSingleton()
    {
        // The Default accessor is the entry point CreateParallelBulkInserterAsync
        // uses when no options are supplied; pin that it is a stable singleton.
        Assert.Same(ParallelBulkInsertOptions.Default, ParallelBulkInsertOptions.Default);
        Assert.Null(ParallelBulkInsertOptions.Default.DegreeOfParallelism);
    }
}
