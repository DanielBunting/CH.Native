using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Mapping;
using CH.Native.Results;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// Pins what happens when a streaming method (<c>QueryAsync</c>,
/// <c>ExecuteReaderAsync</c>, <c>BulkInsertAsync(IAsyncEnumerable)</c>) is interrupted
/// mid-flight. The library docstrings warn that "transient failures will propagate
/// to the caller" once streaming begins; these tests pin the *shape* of that
/// propagation so regressions (hangs, wrong exception types, silent corruption)
/// are caught.
/// </summary>
public class StreamFailureTests
{
    // -----------------------------------------------------------------------
    // Tests against a Toxiproxy-fronted CH for network-level interruption.
    // -----------------------------------------------------------------------
    [Collection("Toxiproxy")]
    [Trait(Categories.Name, Categories.Streams)]
    public class WithChaos : IAsyncLifetime
    {
        private readonly ToxiproxyFixture _proxy;
        private readonly ITestOutputHelper _output;

        public WithChaos(ToxiproxyFixture proxy, ITestOutputHelper output)
        {
            _proxy = proxy;
            _output = output;
        }

        public Task InitializeAsync() => _proxy.ResetProxyAsync();
        public Task DisposeAsync() => _proxy.ResetProxyAsync();

        [Fact]
        public async Task QueryAsync_NetworkResetMidStream_ThrowsTypedException()
        {
            await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
            await conn.OpenAsync();

            // Throttle downstream so the iterator is in-flight when we kick the socket.
            // 4096 = 4 MB/s; Toxiproxy 'rate' is KB/s.
            await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "bandwidth", "downstream",
                new() { ["rate"] = 4096 });

            var injector = Task.Run(async () =>
            {
                await Task.Delay(300);
                await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "reset_peer", "downstream",
                    new() { ["timeout"] = 0 });
            });

            int seen = 0;
            Exception? caught = null;
            try
            {
                await foreach (var row in conn.QueryAsync("SELECT number FROM numbers(2000000)"))
                {
                    _ = row.GetFieldValue<ulong>(0);
                    seen++;
                }
            }
            catch (Exception ex) { caught = ex; }
            finally { await injector; }

            _output.WriteLine($"Streamed {seen} rows before failure; exception: {caught?.GetType().Name}: {caught?.Message}");

            // 1. The stream must have actually started before we broke it; a pre-stream
            //    failure would mean the test isn't testing what it claims.
            Assert.True(seen >= 1, $"Expected ≥ 1 row before reset; saw {seen} — failure was pre-stream, not mid-stream.");

            // 2. The failure must surface as a typed connection-family exception, not
            //    a raw Exception, NRE, or anything else.
            Assert.NotNull(caught);
            Assert.True(
                caught is ClickHouseConnectionException
                || caught is IOException
                || caught is System.Net.Sockets.SocketException
                || caught.InnerException is ClickHouseConnectionException
                || caught.InnerException is IOException
                || caught.InnerException is System.Net.Sockets.SocketException,
                $"Unexpected exception shape: {caught.GetType().FullName}: {caught.Message}");

            // 3. The library must classify this exception as connection-poisoning so
            //    callers using ResilientConnection get a reconnect on next call.
            Assert.True(CH.Native.Resilience.RetryPolicy.IsConnectionPoisoning(caught),
                $"Mid-stream network failure should be connection-poisoning; was {caught.GetType().Name}.");

            // 4. Server health: clear chaos, fresh connection works.
            await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
            await using var fresh = new ClickHouseConnection(_proxy.BuildSettings());
            await fresh.OpenAsync();
            Assert.Equal(1, await fresh.ExecuteScalarAsync<int>("SELECT 1"));
        }

        [Fact]
        public async Task ExecuteReaderAsync_NetworkResetMidRead_ThrowsTypedException()
        {
            await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
            await conn.OpenAsync();

            // 4096 = 4 MB/s; Toxiproxy 'rate' is KB/s.
            await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "bandwidth", "downstream",
                new() { ["rate"] = 4096 });

            var injector = Task.Run(async () =>
            {
                await Task.Delay(300);
                await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "reset_peer", "downstream",
                    new() { ["timeout"] = 0 });
            });

            int reads = 0;
            Exception? caught = null;
            ClickHouseDataReader? failedReader = null;
            try
            {
                var reader = await conn.ExecuteReaderAsync("SELECT number FROM numbers(2000000)");
                failedReader = reader;
                try
                {
                    while (await reader.ReadAsync())
                    {
                        _ = reader.GetFieldValue<ulong>(0);
                        reads++;
                    }
                }
                finally
                {
                    await reader.DisposeAsync();
                }
            }
            catch (Exception ex) { caught = ex; }
            finally { await injector; }

            _output.WriteLine($"Reader pulled {reads} rows before failure; exception: {caught?.GetType().Name}");

            // 1. Failure must be mid-read, not pre-read.
            Assert.True(reads >= 1, $"Expected ≥ 1 read before reset; saw {reads}.");

            // 2. Typed connection-family exception, not raw Exception or NRE.
            Assert.NotNull(caught);
            Assert.True(
                caught is ClickHouseConnectionException
                || caught is IOException
                || caught is System.Net.Sockets.SocketException
                || caught.InnerException is ClickHouseConnectionException
                || caught.InnerException is IOException
                || caught.InnerException is System.Net.Sockets.SocketException,
                $"Unexpected exception shape: {caught.GetType().FullName}: {caught.Message}");

            // 3. Reader must report itself closed after the failure (no half-state).
            Assert.NotNull(failedReader);
            Assert.True(failedReader!.IsClosed,
                "Reader should be IsClosed=true after a mid-stream network failure + disposal.");

            // 4. Server health survives: fresh connection works.
            await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
            await using var fresh = new ClickHouseConnection(_proxy.BuildSettings());
            await fresh.OpenAsync();
            Assert.Equal(1, await fresh.ExecuteScalarAsync<int>("SELECT 1"));
        }

        [Fact]
        public async Task BulkInsertAsyncEnumerable_NetworkResetMidStream_NothingCommitted()
        {
            // Pin the all-or-nothing contract for a single bulk-insert call. The
            // ClickHouse native protocol commits an INSERT only when the server
            // receives the empty terminator block. Blocks streamed before the
            // terminator are held server-side as uncommitted temp parts (MergeTree)
            // or in MemorySink::new_blocks. If the connection is reset mid-stream,
            // the executor is cancelled and MergeTreeSink::~MergeTreeSink calls
            // partition.temp_part->cancel() on every accumulated partition.
            // Result: zero rows visible. Callers who need partial-commit semantics
            // must split the source into multiple BulkInsertAsync calls.
            var table = $"streams_bi_async_{Guid.NewGuid():N}";
            await using var setup = new ClickHouseConnection(_proxy.BuildSettings());
            await setup.OpenAsync();
            await setup.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, payload String) ENGINE = MergeTree ORDER BY id");

            try
            {
                // Throttle upstream so the bulk insert is in-flight when we kick the
                // socket. Rate is generous enough for several 500-row batches to land
                // server-side as uncommitted parts before the reset, so we exercise
                // the "discard accumulated parts" path rather than the "never sent
                // anything" path.
                await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "bandwidth", "upstream",
                    new() { ["rate"] = 512 }); // 512 KB/s — Toxiproxy 'rate' is KB/s, not bytes/s

                var injector = Task.Run(async () =>
                {
                    await Task.Delay(400);
                    await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "reset_peer", "downstream",
                        new() { ["timeout"] = 0 });
                });

                async IAsyncEnumerable<Row> Source()
                {
                    var s = new string('x', 256);
                    for (int i = 0; i < 100_000; i++)
                    {
                        yield return new Row { Id = i, Payload = s };
                        if (i % 1000 == 0) await Task.Yield();
                    }
                }

                Exception? caught = null;
                try
                {
                    // Compression off: the 'x'-padded payload compresses ~30:1 under LZ4,
                    // which makes the 512 KB/s bandwidth toxic finish the whole stream
                    // before the 400 ms reset_peer lands and the test no-ops. Disabling
                    // compression here keeps on-wire bytes ≈ raw bytes so the toxic
                    // actually throttles the insert into the reset window.
                    await using var conn = new ClickHouseConnection(
                        _proxy.BuildSettings(b => b.WithCompression(false)));
                    await conn.OpenAsync();
                    await conn.BulkInsertAsync(table, Source(),
                        new BulkInsertOptions { BatchSize = 500 });
                }
                catch (Exception ex) { caught = ex; }
                finally { await injector; }

                // 1. Typed connection-family exception (or server-side; library may
                //    surface either depending on which side detects the broken pipe
                //    first — we accept both, but never a raw Exception/NRE).
                Assert.NotNull(caught);
                Assert.True(
                    caught is ClickHouseConnectionException
                    || caught is IOException
                    || caught is System.Net.Sockets.SocketException
                    || caught is ClickHouseServerException
                    || caught.InnerException is ClickHouseConnectionException
                    || caught.InnerException is IOException
                    || caught.InnerException is System.Net.Sockets.SocketException
                    || caught.InnerException is ClickHouseServerException,
                    $"Unexpected exception shape: {caught.GetType().FullName}: {caught.Message}");

                // 2. Audit committed rows: must be exactly zero. Any non-zero count
                //    would mean the server committed an INSERT without ever receiving
                //    its terminator, which would be a server-protocol bug. If a future
                //    library change adds per-batch atomicity (see AtomicBatches
                //    proposal), it must ship a separate test — this one stays as the
                //    pin for the default single-statement contract.
                await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
                await using var verify = new ClickHouseConnection(_proxy.BuildSettings());
                await verify.OpenAsync();
                var committed = await verify.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
                _output.WriteLine($"Async bulk-insert mid-stream failure: committed {committed}/100000, exception {caught.GetType().Name}");

                Assert.Equal(0UL, committed);
            }
            finally
            {
                await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
                await using var teardown = new ClickHouseConnection(_proxy.BuildSettings());
                await teardown.OpenAsync();
                await teardown.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Tests against a single CH (no proxy) for server-side mid-stream errors.
    // -----------------------------------------------------------------------
    [Collection("SingleNode")]
    [Trait(Categories.Name, Categories.Streams)]
    public class WithServerErrors
    {
        private readonly SingleNodeFixture _fixture;
        private readonly ITestOutputHelper _output;

        public WithServerErrors(SingleNodeFixture fixture, ITestOutputHelper output)
        {
            _fixture = fixture;
            _output = output;
        }

        [Fact]
        public async Task QueryAsync_ServerSideMemoryLimit_SurfacesAsClickHouseServerException()
        {
            // A heavy aggregation under a tight max_memory_usage will produce some
            // intermediate output (or trigger early), then the server emits an
            // Exception packet. The iterator should surface that as a server
            // exception, NOT as a wire-level IOException.
            await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
            await conn.OpenAsync();

            int rows = 0;
            Exception? caught = null;
            try
            {
                // groupArray on a huge sequence forces big in-memory state per block.
                await foreach (var r in conn.QueryAsync(
                    "SELECT groupArray(number) FROM numbers(10000000) " +
                    "GROUP BY number % 1000 " +
                    "SETTINGS max_memory_usage = 500000"))
                {
                    _ = r.GetFieldValue<ulong[]>(0);
                    rows++;
                }
            }
            catch (Exception ex) { caught = ex; }

            _output.WriteLine($"Yielded {rows} rows before server error; exception: {caught?.GetType().Name}: {caught?.Message}");
            Assert.NotNull(caught);

            var server = caught as ClickHouseServerException
                         ?? caught.InnerException as ClickHouseServerException;
            Assert.NotNull(server);
            // 241 = MEMORY_LIMIT_EXCEEDED
            Assert.Equal(241, server!.ErrorCode);

            // Connection should still be usable AFTER the server-side exception
            // (server emits Exception then EndOfStream cleanly).
            var ok = await conn.ExecuteScalarAsync<int>("SELECT 1");
            Assert.Equal(1, ok);
        }

        [Fact]
        public async Task QueryAsync_ServerExecutionTimeout_SurfacesCleanly()
        {
            // max_execution_time triggers TIMEOUT_EXCEEDED (159) — also non-poisoning.
            await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
            await conn.OpenAsync();

            int rows = 0;
            Exception? caught = null;
            try
            {
                await foreach (var r in conn.QueryAsync(
                    "SELECT number FROM numbers(100000000000) " +
                    "SETTINGS max_execution_time = 1, timeout_overflow_mode = 'throw'"))
                {
                    _ = r.GetFieldValue<ulong>(0);
                    rows++;
                }
            }
            catch (Exception ex) { caught = ex; }

            _output.WriteLine($"Yielded {rows} rows before server timeout; exception: {caught?.GetType().Name}");
            Assert.NotNull(caught);
            var server = caught as ClickHouseServerException
                         ?? caught.InnerException as ClickHouseServerException;
            Assert.NotNull(server);
            Assert.Equal(159, server!.ErrorCode);

            // Connection still usable.
            var ok = await conn.ExecuteScalarAsync<int>("SELECT 1");
            Assert.Equal(1, ok);
        }
    }

    private class Row
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "payload", Order = 1)] public string Payload { get; set; } = "";
    }
}
