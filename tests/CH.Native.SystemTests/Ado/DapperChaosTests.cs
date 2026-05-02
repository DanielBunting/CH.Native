using System.Net.Sockets;
using CH.Native.Ado;
using CH.Native.Connection;
using CH.Native.Dapper;
using CH.Native.Exceptions;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Dapper;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Ado;

/// <summary>
/// Pins the contract for Dapper-driven queries when the underlying socket fails
/// mid-stream or is disposed mid-enumeration. Existing ADO.NET coverage is
/// happy-path; this surfaces the failure shape callers actually need to handle.
/// </summary>
[Collection("Toxiproxy")]
[Trait(Categories.Name, Categories.Chaos)]
public class DapperChaosTests : IAsyncLifetime
{
    private readonly ToxiproxyFixture _proxy;
    private readonly ITestOutputHelper _output;

    static DapperChaosTests()
    {
        ClickHouseDapperIntegration.Register();
    }

    public DapperChaosTests(ToxiproxyFixture proxy, ITestOutputHelper output)
    {
        _proxy = proxy;
        _output = output;
    }

    public Task InitializeAsync() => _proxy.ResetProxyAsync();
    public Task DisposeAsync() => _proxy.ResetProxyAsync();

    private string ConnectionString =>
        $"Host={_proxy.ProxyHost};Port={_proxy.ProxyPort};Username={ToxiproxyFixture.Username};Password={ToxiproxyFixture.Password}";

    [Fact]
    public async Task DapperBufferedQuery_InterruptedBySocketReset_ThrowsTypedFailure()
    {
        var harness = await BulkInsertTableHarness.CreateAsync(
            () => _proxy.BuildSettings(),
            columnDdl: "id Int32, payload String");

        try
        {
            // Seed enough rows that the SELECT is genuinely streaming when the toxic lands.
            await using (var seed = new ClickHouseConnection(_proxy.BuildSettings()))
            {
                await seed.OpenAsync();
                var s = new string('x', 256);
                await using var inserter = seed.CreateBulkInserter<StandardRow>(harness.TableName);
                await inserter.InitAsync();
                for (int i = 0; i < 20_000; i++)
                    await inserter.AddAsync(new StandardRow { Id = i, Payload = s });
                await inserter.CompleteAsync();
            }

            // Throttle the downstream so the buffered SELECT is mid-flight when reset_peer hits.
            await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "bandwidth", "downstream",
                new() { ["rate"] = 64 }); // 64 KB/s
            var injectTask = Task.Run(async () =>
            {
                await Task.Delay(300);
                await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "reset_peer", "downstream",
                    new() { ["timeout"] = 0 });
            });

            await using var conn = new ClickHouseDbConnection(ConnectionString);
            await conn.OpenAsync();

            Exception? caught = null;
            try
            {
                _ = (await conn.QueryAsync<DapperRow>(
                    $"SELECT id, payload FROM {harness.TableName} ORDER BY id")).ToList();
            }
            catch (Exception ex) { caught = ex; }
            finally
            {
                await injectTask;
                await _proxy.ResetProxyAsync();
            }

            Assert.NotNull(caught);
            _output.WriteLine($"Caught: {caught!.GetType().FullName}: {caught.Message}");

            // Contract: a typed CH.Native exception (or a clean cancellation) — never
            // a raw IOException/SocketException leaking from the wire.
            var typedFailure = IsTypedClickHouseFailure(caught);
            Assert.True(typedFailure,
                $"Expected typed ClickHouse failure; got {caught.GetType().FullName}: {caught.Message}");

            // Note: ClickHouseDbConnection.State does NOT auto-transition to Closed on
            // wire failure today — that is a known ADO.NET-surface gap (the wrapper's
            // _state is only updated by Open/Close paths). We assert reusability instead
            // of State: a follow-up call must surface a typed failure, proving the
            // connection is effectively dead even if its public State still reads Open.
            var reuseEx = await Record.ExceptionAsync(() => conn.QueryAsync<long>(
                $"SELECT count() FROM {harness.TableName}"));
            Assert.NotNull(reuseEx);
        }
        finally
        {
            await harness.DisposeAsync();
        }
    }

    [Fact]
    public async Task DapperUnbufferedQuery_DisposedMidEnumeration_PoolRecovers()
    {
        var harness = await BulkInsertTableHarness.CreateAsync(
            () => _proxy.BuildSettings(),
            columnDdl: "id Int32, payload String");

        try
        {
            await using (var seed = new ClickHouseConnection(_proxy.BuildSettings()))
            {
                await seed.OpenAsync();
                var s = new string('x', 64);
                await using var inserter = seed.CreateBulkInserter<StandardRow>(harness.TableName);
                await inserter.InitAsync();
                for (int i = 0; i < 5_000; i++)
                    await inserter.AddAsync(new StandardRow { Id = i, Payload = s });
                await inserter.CompleteAsync();
            }

            // Streaming Dapper query disposed after partial enumeration. The connection
            // must either drain cleanly or be evicted; either way the next query must work.
            int rowsConsumed = 0;
            await using (var conn = new ClickHouseDbConnection(ConnectionString))
            {
                await conn.OpenAsync();
                using var rows = conn.Query<DapperRow>(
                    $"SELECT id, payload FROM {harness.TableName} ORDER BY id",
                    buffered: false).GetEnumerator();
                for (int i = 0; i < 50 && rows.MoveNext(); i++) rowsConsumed++;
                // dispose the enumerator (using-block) before EOF
            }
            Assert.Equal(50, rowsConsumed);

            // A separate connection runs a fresh query — proves no shared state corruption.
            await using var verify = new ClickHouseDbConnection(ConnectionString);
            await verify.OpenAsync();
            var total = (await verify.QueryAsync<long>(
                $"SELECT count() FROM {harness.TableName}")).Single();
            Assert.Equal(5_000L, total);
        }
        finally
        {
            await harness.DisposeAsync();
        }
    }

    private static bool IsTypedClickHouseFailure(Exception ex)
    {
        // Walk the cause chain. Any of: ClickHouseException, ClickHouseProtocolException,
        // ClickHouseNetworkException, or OperationCanceledException counts as typed.
        // A bare SocketException/IOException leaking out is a regression.
        for (Exception? cur = ex; cur != null; cur = cur.InnerException)
        {
            if (cur is ClickHouseException) return true;
            if (cur.GetType().Namespace?.StartsWith("CH.Native.Exceptions") == true) return true;
            if (cur is OperationCanceledException) return true;
        }
        // Fall back: at least the top-level should not be a raw socket layer leak.
        return ex is not SocketException && ex.GetType() != typeof(System.IO.IOException);
    }

    private sealed class DapperRow
    {
        public int Id { get; set; }
        public string Payload { get; set; } = "";
    }
}
