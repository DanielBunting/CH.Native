using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pins the busy-slot contract for bulk insert: while a <see cref="BulkInserter{T}"/>
/// holds the wire (from <c>InitAsync</c> until <c>CompleteAsync</c>/<c>DisposeAsync</c>),
/// any concurrent <c>QueryAsync</c> / <c>ExecuteNonQueryAsync</c> on the same
/// connection must throw <see cref="ClickHouseConnectionBusyException"/> rather than
/// corrupting the INSERT byte stream.
///
/// <para>
/// The cancellation suite already covers cancel→drain races. This file specifically
/// covers the <em>collision-rejection</em> path: a second caller hitting the same
/// connection while the first is mid-insert must be deterministically rejected with
/// the typed exception, and the in-flight insert must complete cleanly afterwards.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class BulkInsertBusyCollisionTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public BulkInsertBusyCollisionTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task QueryDuringActiveBulkInsert_ThrowsBusy_AndInsertCompletes()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 1000 });
        await inserter.InitAsync();

        // Pin some rows in the buffer — the inserter holds the busy slot
        // continuously between InitAsync and CompleteAsync.
        for (int i = 0; i < 100; i++)
            await inserter.AddAsync(new StandardRow { Id = i, Payload = "x" });

        // Concurrent query on the SAME connection — must be rejected synchronously
        // by the EnterBusy gate. We don't drive it from a Task.Run here because
        // the gate is a synchronous lock check; a direct call surfaces the typed
        // exception without scheduler noise.
        var ex = await Assert.ThrowsAsync<ClickHouseConnectionBusyException>(async () =>
        {
            await foreach (var _ in conn.QueryAsync<int>("SELECT 1")) { }
        });

        _output.WriteLine($"Busy-collision threw: {ex.GetType().Name}: {ex.Message}");

        // The original insert must still complete cleanly — the rejected
        // concurrent caller should not have perturbed the wire state.
        await inserter.CompleteAsync();

        Assert.Equal(100UL, await harness.CountAsync());
    }

    [Fact]
    public async Task ExecuteNonQueryDuringActiveBulkInsert_ThrowsBusy()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 1000 });
        await inserter.InitAsync();

        await Assert.ThrowsAsync<ClickHouseConnectionBusyException>(async () =>
        {
            await conn.ExecuteNonQueryAsync($"SELECT count() FROM {harness.TableName}");
        });

        await inserter.AddAsync(new StandardRow { Id = 1, Payload = "x" });
        await inserter.CompleteAsync();
        Assert.Equal(1UL, await harness.CountAsync());
    }

    [Fact]
    public async Task SecondBulkInserterDuringActiveBulkInsert_ThrowsBusyAtInit()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        await using var first = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 1000 });
        await first.InitAsync();

        // Try to start a second inserter on the same connection while the first
        // still owns the slot. EnterBusyForBulkInsert in InitAsync should reject.
        await Assert.ThrowsAsync<ClickHouseConnectionBusyException>(async () =>
        {
            await using var second = conn.CreateBulkInserter<StandardRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 1000 });
            await second.InitAsync();
        });

        // First inserter unaffected — still drives the original wire conversation.
        await first.AddAsync(new StandardRow { Id = 1, Payload = "x" });
        await first.CompleteAsync();
        Assert.Equal(1UL, await harness.CountAsync());
    }

    [Fact]
    public async Task SlotReleasedAfterCompleteAsync_NextQuerySucceeds()
    {
        // Lock-in: CompleteAsync releases the busy slot before the inserter is
        // disposed, so callers can issue another query on the same connection
        // before the `await using` finalizes (BulkInserter.cs lines 610-614).
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        await using (var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 1000 }))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = "x" });
            await inserter.CompleteAsync();

            // Inside the using block but post-Complete: the slot is released,
            // so a non-query operation on the same connection must succeed
            // (locks in BulkInserter.cs:610-614 pre-dispose slot release).
            // We use ExecuteScalarAsync<int> which is well-mapped; counting via
            // a fresh connection (harness.CountAsync) is the audit oracle.
            await conn.ExecuteScalarAsync<int>("SELECT 1");
        }

        Assert.Equal(1UL, await harness.CountAsync());
    }
}
