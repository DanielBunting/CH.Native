using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Mapping;
using CH.Native.Resilience;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Chaos;

/// <summary>
/// What happens when a bulk insert is severed mid-flight by a network failure?
/// Documents the partial-commit reality and asserts the error is descriptive.
/// </summary>
[Collection("Toxiproxy")]
[Trait(Categories.Name, Categories.Chaos)]
public class BulkInsertChaosTests : IAsyncLifetime
{
    private readonly ToxiproxyFixture _proxy;
    private readonly ITestOutputHelper _output;

    public BulkInsertChaosTests(ToxiproxyFixture proxy, ITestOutputHelper output)
    {
        _proxy = proxy;
        _output = output;
    }

    public Task InitializeAsync() => _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
    public Task DisposeAsync() => _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);

    [Fact]
    public async Task ResetMidFlush_ProducesDescriptiveError_DocumentsCommittedRowCount()
    {
        var table = $"chaos_bi_{Guid.NewGuid():N}";

        await using var setup = new ClickHouseConnection(_proxy.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, payload String) ENGINE = MergeTree ORDER BY id");

        try
        {
            // Throttle the upstream so the bulk insert is in-flight long enough that
            // the reset injection lands mid-stream rather than after completion.
            await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "bandwidth", "upstream",
                new() { ["rate"] = 1024 }); // 1 MB/s (Toxiproxy 'rate' is KB/s)

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
                await using var inserter = conn.CreateBulkInserter<Row>(table,
                    new BulkInsertOptions { BatchSize = 500 });
                await inserter.InitAsync();

                var s = new string('x', 256);
                for (int i = 0; i < 100_000; i++)
                    await inserter.AddAsync(new Row { Id = i, Payload = s });
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
            // Classify the exception: must be either connection-poisoning (most likely)
            // or a server-side error. Generic Exception or NRE means we're not surfacing
            // anything actionable.
            var classified =
                RetryPolicy.IsConnectionPoisoning(caught)
                || caught is ClickHouseServerException
                || caught.InnerException is ClickHouseServerException;
            Assert.True(classified,
                $"Mid-flush failure should be a typed connection-poisoning or server exception; got {caught.GetType().FullName}: {caught.Message}");

            // Audit committed rows. Strict bounds:
            //   - committed < 100_000 — the reset must have actually interrupted the insert.
            //   - if the library claims atomic-batch semantics (MergeTree), committed is
            //     a multiple of BatchSize. Pin that contract.
            await using var auditConn = new ClickHouseConnection(_proxy.BuildSettings());
            await auditConn.OpenAsync();
            var committed = await auditConn.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            _output.WriteLine($"Committed rows after mid-flush reset: {committed} / 100000 attempted");

            Assert.True(committed < 100_000UL,
                $"Reset injection didn't actually interrupt — full {committed} rows landed.");
            const int batchSize = 500;
            Assert.True(committed % (ulong)batchSize == 0,
                $"Committed rows ({committed}) is not a multiple of BatchSize ({batchSize}) — torn batch detected.");
        }
        finally
        {
            await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
            await using var teardown = new ClickHouseConnection(_proxy.BuildSettings());
            await teardown.OpenAsync();
            await teardown.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private class Row
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "payload", Order = 1)] public string Payload { get; set; } = "";
    }
}
