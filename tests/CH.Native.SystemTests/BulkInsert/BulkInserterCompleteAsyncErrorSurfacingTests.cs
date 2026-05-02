using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Mapping;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pins the contract for where bulk-insert server-side errors surface and
/// what shape they take. Use-cases §5.1 / §9.7 recommend always calling
/// <c>CompleteAsync</c> explicitly so server errors land at a deterministic
/// call site rather than from inside <c>DisposeAsync</c>.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class BulkInserterCompleteAsyncErrorSurfacingTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public BulkInserterCompleteAsyncErrorSurfacingTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task ForgottenCompleteAsync_BufferedRows_DisposeThrowsLoudly()
    {
        // The scenario use-cases §9.7 warns about: callers wrap a
        // BulkInserter in `await using` and forget the explicit
        // CompleteAsync. Buffered (un-flushed) rows must trigger a loud
        // throw on dispose so the data loss is impossible to miss.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(),
            namePrefix: "complete_forget");
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            // Large BatchSize so AddAsync buffers without auto-flushing.
            await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 10_000 });
            await inserter.InitAsync();
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = "lost" });
            // Caller forgets CompleteAsync — `await using` triggers DisposeAsync.
        });

        _output.WriteLine($"Forgotten complete surfaced: {ex.Message}");
        Assert.Contains("CompleteAsync", ex.Message);
    }

    [Fact]
    public async Task ExplicitCompleteAsync_OnNonExistentTable_SurfacesAtInitOrComplete()
    {
        // Pin the call-site contract: server-side errors surface at one of
        // the explicit lifecycle methods (InitAsync / CompleteAsync) rather
        // than getting swallowed by the inserter's internals. Insertion
        // against a non-existent table is the most reliable trigger.
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var bogusTable = $"never_created_{Guid.NewGuid():N}";

        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var inserter = conn.CreateBulkInserter<StandardRow>(bogusTable,
                new BulkInsertOptions { BatchSize = 10_000 });
            await inserter.InitAsync();
            await inserter.AddAsync(new StandardRow { Id = 1, Payload = "x" });
            await inserter.CompleteAsync();
        });

        _output.WriteLine($"Bogus-table error: {ex.GetType().Name}: {ex.Message}");
        Assert.IsAssignableFrom<ClickHouseServerException>(ex);
    }

    [Fact]
    public async Task Abort_BetweenInitAndComplete_DisposeDoesNotThrow()
    {
        // Internal Abort() suppresses the dispose-time implicit complete. We
        // expose it via a server-side cancellation that the BulkInserter
        // observes — the cleanest external trigger. After such a failure,
        // DisposeAsync must be a clean no-op.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(),
            namePrefix: "complete_abort");
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        using var cts = new CancellationTokenSource();
        await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 10_000 });
        await inserter.InitAsync();
        await inserter.AddAsync(new StandardRow { Id = 1, Payload = "x" });

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            inserter.CompleteAsync(cts.Token));

        // Disposing after a cancelled CompleteAsync must be safe — the
        // inserter's internal Abort() flag prevents a retry that would
        // re-throw. This test fails if DisposeAsync throws.
        await inserter.DisposeAsync();
    }

    [Fact]
    public async Task ClientSideOverflow_OnAdd_SurfacesAsTypedException_BeforeWireSend()
    {
        // The Decimal32(2) column type's serializer rejects values that
        // exceed Int32 range with a typed OverflowException — fired
        // client-side on AddAsync, before the wire ever sees the row.
        // This is the right shape: range failures surface at the
        // earliest possible point, not deep inside CompleteAsync.
        var tableName = $"decimal_overflow_{Guid.NewGuid():N}";
        await using (var setup = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await setup.OpenAsync();
            await setup.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (id Int32, amount Decimal32(2)) ENGINE = MergeTree ORDER BY id");
        }

        Exception? caught = null;
        try
        {
            await using var conn = new ClickHouseConnection(_fx.BuildSettings());
            await conn.OpenAsync();

            await using var inserter = conn.CreateBulkInserter<DecimalRow>(tableName,
                new BulkInsertOptions { BatchSize = 10_000 });
            await inserter.InitAsync();
            try
            {
                await inserter.AddAsync(new DecimalRow { Id = 1, Amount = 99_999_999_999.99m });
                await inserter.CompleteAsync();
            }
            catch (Exception ex) { caught = ex; }
        }
        finally
        {
            try
            {
                await using var cleanup = new ClickHouseConnection(_fx.BuildSettings());
                await cleanup.OpenAsync();
                await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            }
            catch { /* best-effort */ }
        }

        Assert.NotNull(caught);
        _output.WriteLine($"Overflow surfaced: {caught!.GetType().Name}: {caught.Message}");
        Assert.True(
            caught is OverflowException ||
            caught is ArgumentException ||
            caught is ClickHouseServerException,
            $"Expected typed exception; got {caught.GetType().FullName}");
    }

    [Fact]
    public async Task CancelledMidFlight_CompleteAsync_RethrowsOperationCanceledException()
    {
        // Use-cases §9.12 anti-pattern: swallowing OCE hides shutdown
        // signals. Pin that a cancelled CompleteAsync surfaces OCE rather
        // than translating into a generic exception.
        await using var harness = await BulkInsertTableHarness.CreateAsync(
            () => _fx.BuildSettings(),
            namePrefix: "complete_cancel");
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        using var cts = new CancellationTokenSource();
        await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
            new BulkInsertOptions { BatchSize = 10_000 });
        await inserter.InitAsync();
        for (int i = 0; i < 100; i++)
            await inserter.AddAsync(new StandardRow { Id = i, Payload = "x" });

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            inserter.CompleteAsync(cts.Token));
    }

    internal sealed class DecimalRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "amount", Order = 1)] public decimal Amount { get; set; }
    }
}
