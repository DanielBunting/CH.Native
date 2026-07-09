using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pins the lazy-initialization contract: <see cref="BulkInserter{T}"/> and
/// <see cref="DynamicBulkInserter"/> initialize (send the INSERT query, resolve
/// the schema, claim the busy slot) at the first mutating call — no explicit
/// <c>InitAsync</c> required. <c>InitAsync</c> remains as an optional,
/// idempotent eager-validation step.
///
/// <para>
/// The lazy path must preserve every wire-state invariant of the explicit path:
/// a failed first-Add init drains and releases the slot (connection reusable), a
/// pre-cancelled token at first Add is a plain throw with no server-side query,
/// and a never-initialized inserter's Complete/Dispose never contacts the server.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class BulkInsertLazyInitTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public BulkInsertLazyInitTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task LazyInit_Typed_AddThenComplete_NoInitAsync_RoundTrips()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // Small batch size so the run crosses several auto-flushes — proving
        // the lazily-established INSERT stream survives multiple data blocks.
        await using (var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 10 }))
        {
            for (int i = 0; i < 25; i++)
                await inserter.AddAsync(new StandardRow { Id = i, Payload = "lazy" });
            await inserter.CompleteAsync();
        }

        Assert.Equal(25UL, await harness.CountAsync());
        // Slot released, wire idle — the same connection answers immediately.
        Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
    }

    [Fact]
    public async Task LazyInit_Dynamic_AddThenComplete_NoInitAsync_RoundTrips()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        await using (var inserter = conn.CreateBulkInserter(harness.TableName,
            new[] { "id", "payload" }, new BulkInsertOptions { BatchSize = 10 }))
        {
            for (int i = 0; i < 25; i++)
                await inserter.AddAsync(new object?[] { i, "lazy" });
            await inserter.CompleteAsync();
        }

        Assert.Equal(25UL, await harness.CountAsync());
        Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
    }

    [Fact]
    public async Task LazyInit_TableMissing_FirstAddThrowsServerException_ConnectionReusable()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var missingTable = $"definitely_not_a_table_{Guid.NewGuid():N}";
        var inserter = conn.CreateBulkInserter<StandardRow>(missingTable);

        // The server error that used to surface at InitAsync now surfaces at
        // the first Add — same exception type, moved callsite.
        var ex = await Assert.ThrowsAsync<ClickHouseServerException>(async () =>
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = "x" }));
        _output.WriteLine($"First Add threw: {ex.Message}");

        // Failed init must not latch the inserter as initialized (retryable)…
        Assert.False(InserterStateInspector.Initialized(inserter));
        await inserter.DisposeAsync();

        // …and must have drained the wire + released the busy slot, mirroring
        // BulkInserterInitFailureCleanupTests for the explicit path.
        Assert.Equal(42, await conn.ExecuteScalarAsync<int>("SELECT 42"));

        // Full witness: a fresh inserter on the SAME connection completes a
        // clean lazy insert into a real table.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using (var second = conn.CreateBulkInserter<StandardRow>(harness.TableName))
        {
            await second.AddAsync(new StandardRow { Id = 1, Payload = "y" });
            await second.CompleteAsync();
        }
        Assert.Equal(1UL, await harness.CountAsync());
    }

    [Fact]
    public async Task LazyInit_CancelledTokenAtFirstAdd_PlainOCE_NoServerSideQuery()
    {
        // Pre-init, a cancelled token at the first Add must be a plain throw —
        // the lazy init runs BEFORE ObserveCancellationAsync, so no Cancel
        // packet and no INSERT query ever reach the server. Mirrors
        // CancelRecoveryTests.BulkInsert_CancelBeforeInit_ThrowsCleanly for
        // the explicit-InitAsync path.
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // dead at entry

        await using (var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 1000 }))
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await inserter.AddAsync(new StandardRow { Id = 1, Payload = "x" }, cts.Token));
        }

        // Same connection: scalar still works (nothing was sent, nothing to drain).
        Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));

        // No INSERT query should have been registered server-side. Poll briefly
        // for race tolerance (pattern from CancelRecoveryTests).
        var deadline = DateTime.UtcNow.AddSeconds(2);
        ulong stuckQueries = 1;
        while (DateTime.UtcNow < deadline)
        {
            stuckQueries = await conn.ExecuteScalarAsync<ulong>(
                $"SELECT count() FROM system.processes WHERE query LIKE 'INSERT INTO {harness.TableName}%'");
            if (stuckQueries == 0) break;
            await Task.Delay(100);
        }
        Assert.Equal(0UL, stuckQueries);
        Assert.Equal(0UL, await harness.CountAsync());

        // The same connection can drive a complete lazy bulk insert afterwards.
        await using (var second = conn.CreateBulkInserter<StandardRow>(harness.TableName))
        {
            await second.AddAsync(new StandardRow { Id = 1, Payload = "y" });
            await second.CompleteAsync();
        }
        Assert.Equal(1UL, await harness.CountAsync());
    }

    [Fact]
    public async Task NeverInited_CompleteAndDispose_NoWireActivity_ConnectionImmediatelyReusable()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName);
        // Zero adds, no InitAsync: complete is a silent no-op — no INSERT, no
        // terminator, no busy slot ever claimed. (Contrast with the explicit
        // path in BulkInsertDisposalRaceTests, where InitAsync + dispose still
        // drives a full zero-row round trip.)
        await inserter.CompleteAsync();

        // The connection was never made busy, so it answers even before the
        // inserter is disposed.
        Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
        await inserter.DisposeAsync();

        Assert.Equal(0UL, await harness.CountAsync());
    }

    [Fact]
    public async Task InitAsync_Idempotent_ExplicitThenLazy_AndLazyThenExplicit()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // (a) Double explicit init, then rows: second InitAsync is a no-op
        // (used to throw "already initialized").
        await using (var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName))
        {
            await inserter.InitAsync();
            await inserter.InitAsync();
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = "a" });
            await inserter.CompleteAsync();
        }

        // (b) Lazy init via Add, then explicit InitAsync mid-stream: must be a
        // no-op (no second INSERT query on the wire), and the stream continues.
        await using (var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName))
        {
            await inserter.AddAsync(new StandardRow { Id = 2, Payload = "b" });
            await inserter.InitAsync();
            await inserter.AddAsync(new StandardRow { Id = 3, Payload = "b" });
            await inserter.CompleteAsync();
        }

        Assert.Equal(3UL, await harness.CountAsync());
    }

    [Fact]
    public async Task LazyInit_SchemaCacheHit_SkipsSchemaRoundTrip()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fixture.BuildSettings());
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var options = new BulkInsertOptions { UseSchemaCache = true };

        // First inserter warms the connection's schema cache (lazy path).
        await using (var first = conn.CreateBulkInserter<StandardRow>(harness.TableName, options))
        {
            await first.AddAsync(new StandardRow { Id = 1, Payload = "warm" });
            await first.CompleteAsync();
        }

        // Second inserter's lazy init takes the cache-hit branch inside
        // EnsureInitializedAsync (skips the schema read round-trip) and must
        // still produce a correct insert.
        await using (var second = conn.CreateBulkInserter<StandardRow>(harness.TableName, options))
        {
            await second.AddAsync(new StandardRow { Id = 2, Payload = "hit" });
            await second.CompleteAsync();
        }

        Assert.Equal(2UL, await harness.CountAsync());
    }
}
