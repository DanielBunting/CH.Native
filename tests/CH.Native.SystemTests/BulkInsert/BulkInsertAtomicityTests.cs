using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Resilience;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pins the per-block atomicity contract: the server's commit boundary is the data
/// block, so the only valid post-failure committed-row counts are multiples of the
/// inserter's <c>BatchSize</c>. Promotes the assertion previously embedded in
/// <see cref="Chaos.BulkInsertChaosTests"/> to its own focused suite and adds the
/// post-ack and throttled-flush variants.
/// </summary>
[Collection("Toxiproxy")]
[Trait(Categories.Name, Categories.Chaos)]
public class BulkInsertAtomicityTests : IAsyncLifetime
{
    private readonly ToxiproxyFixture _proxy;
    private readonly ITestOutputHelper _output;

    public BulkInsertAtomicityTests(ToxiproxyFixture proxy, ITestOutputHelper output)
    {
        _proxy = proxy;
        _output = output;
    }

    public Task InitializeAsync() => _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
    public Task DisposeAsync() => _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);

    [Fact]
    public async Task Reset_BetweenBlocks_CommittedCountIsAlwaysMultipleOfBatchSize()
    {
        // 100k * 256-byte rows ≈ 25 MB; throttled at 1024 KB/s gives ~25 s of
        // upstream traffic, plenty of window for the 500 ms-delayed reset to
        // land mid-stream. Mirrors BulkInsertChaosTests.ResetMidFlush_* sizing.
        const int batchSize = 500;
        const int totalRows = 100_000;

        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _proxy.BuildSettings());

        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "bandwidth", "upstream",
            new() { ["rate"] = 1024 });

        var injectTask = Task.Run(async () =>
        {
            await Task.Delay(500);
            await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "reset_peer", "downstream",
                new() { ["timeout"] = 0 });
        });

        Exception? caught = null;
        try
        {
            await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
            await conn.OpenAsync();
            await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = batchSize });
            await inserter.InitAsync();

            var s = new string('x', 256);
            for (int i = 0; i < totalRows; i++)
                await inserter.AddAsync(new StandardRow { Id = i, Payload = s });
            await inserter.CompleteAsync();
        }
        catch (Exception ex)
        {
            caught = ex;
        }
        finally
        {
            await injectTask;
            await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
        }

        Assert.NotNull(caught);
        _output.WriteLine($"Inserter surfaced: {caught!.GetType().Name}: {caught.Message}");

        var classified =
            RetryPolicy.IsConnectionPoisoning(caught)
            || caught is ClickHouseServerException
            || caught.InnerException is ClickHouseServerException;
        Assert.True(classified,
            $"Mid-flush failure should be a typed connection-poisoning or server exception; got {caught.GetType().FullName}: {caught.Message}");

        var committed = await harness.CountAsync();
        _output.WriteLine($"Committed rows after mid-flush reset: {committed} / {totalRows} attempted");

        Assert.True(committed < (ulong)totalRows,
            $"Reset injection didn't actually interrupt — full {committed} rows landed.");
        Assert.True(committed % (ulong)batchSize == 0,
            $"Committed rows ({committed}) is not a multiple of BatchSize ({batchSize}) — torn batch detected.");
    }

    [Fact]
    public async Task Reset_DuringFinalSmallBlock_CommittedIsZeroOrFullOrFinalBoundary()
    {
        // BatchSize=1000, total=1500 means the server will see at most one full
        // block (rows 0..999) and one short final block (rows 1000..1499) sent
        // by CompleteAsync. The reset can land before the first block, between
        // them, or after both. Valid committed counts are therefore exactly
        // {0, 1000, 1500} — anything else means a torn block.
        const int batchSize = 1000;
        const int totalRows = 1500;

        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _proxy.BuildSettings());

        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "bandwidth", "upstream",
            new() { ["rate"] = 256 });

        var injectTask = Task.Run(async () =>
        {
            await Task.Delay(300);
            await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "reset_peer", "downstream",
                new() { ["timeout"] = 0 });
        });

        try
        {
            await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
            await conn.OpenAsync();
            await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = batchSize });
            await inserter.InitAsync();

            var s = new string('x', 512);
            for (int i = 0; i < totalRows; i++)
                await inserter.AddAsync(new StandardRow { Id = i, Payload = s });
            await inserter.CompleteAsync();
        }
        catch (Exception ex)
        {
            _output.WriteLine($"Inserter surfaced: {ex.GetType().Name}: {ex.Message}");
        }
        finally
        {
            await injectTask;
            await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
        }

        var committed = await harness.CountAsync();
        _output.WriteLine($"Committed rows: {committed} (expected 0, {batchSize}, or {totalRows})");
        Assert.Contains(committed, new ulong[] { 0UL, (ulong)batchSize, (ulong)totalRows });
    }

    [Fact]
    public async Task LimitDataRate_DuringFlush_NoReset_AllRowsCommitted()
    {
        // Bandwidth-only toxic; no reset. The insert should complete; this pins
        // the contract that throttling alone does not produce a partial commit.
        const int totalRows = 5_000;

        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _proxy.BuildSettings());

        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "bandwidth", "upstream",
            new() { ["rate"] = 256 });

        try
        {
            await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
            await conn.OpenAsync();
            await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 1000 });
            await inserter.InitAsync();

            var s = new string('x', 64);
            for (int i = 0; i < totalRows; i++)
                await inserter.AddAsync(new StandardRow { Id = i, Payload = s });
            await inserter.CompleteAsync();
        }
        finally
        {
            await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
        }

        var committed = await harness.CountAsync();
        Assert.Equal((ulong)totalRows, committed);
    }

    [Fact]
    public async Task Reset_AfterFinalAck_DoesNotRollBackCommittedRows()
    {
        // ClickHouse INSERT commits when the server processes the empty terminator.
        // A reset_peer applied after CompleteAsync returns must not affect the
        // committed-row count — the data is already durable.
        const int totalRows = 2_000;

        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _proxy.BuildSettings());

        await using (var conn = new ClickHouseConnection(_proxy.BuildSettings()))
        {
            await conn.OpenAsync();
            await using var inserter = conn.CreateBulkInserter<StandardRow>(harness.TableName,
                new BulkInsertOptions { BatchSize = 1000 });
            await inserter.InitAsync();
            var s = new string('x', 64);
            for (int i = 0; i < totalRows; i++)
                await inserter.AddAsync(new StandardRow { Id = i, Payload = s });
            await inserter.CompleteAsync();
        }

        // Reset *after* the inserter is fully closed. Should be a no-op for
        // already-committed rows.
        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });
        await Task.Delay(100);
        await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);

        var committed = await harness.CountAsync();
        Assert.Equal((ulong)totalRows, committed);
    }
}
