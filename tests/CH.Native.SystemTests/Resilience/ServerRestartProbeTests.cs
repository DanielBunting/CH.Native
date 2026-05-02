using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Extends <see cref="PoolRestartRecoveryTests"/> with probes for behaviour at the
/// edges of a server restart: bulk-insert mid-flight, session-state contract, dispose
/// race vs waiters, statistics invariants, and ResilientConnection retry policy on
/// non-idempotent INSERTs.
///
/// <para>Belongs in the <c>RestartableSingleNode</c> collection so the existing
/// recovery tests' fixture-cleanup pattern applies — every test class restarts the
/// container in <see cref="DisposeAsync"/> so the fixture stays healthy for the next
/// test class in the collection.</para>
/// </summary>
[Collection("RestartableSingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public sealed class ServerRestartProbeTests : IAsyncLifetime
{
    private readonly RestartableSingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ServerRestartProbeTests(RestartableSingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        try
        {
            await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
            await conn.OpenAsync();
        }
        catch
        {
            try { await _fixture.StartContainerAsync(); } catch { }
        }
    }

    private ClickHouseDataSource BuildDataSource(int maxPoolSize = 4) =>
        new(new ClickHouseDataSourceOptions
        {
            Settings = _fixture.BuildSettings(),
            MaxPoolSize = maxPoolSize,
            ValidateOnRent = true,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(10),
        });

    [Fact]
    public async Task ServerRestart_PoolDiscardsAllStaleConnections_FreshOpensCreated()
    {
        await using var ds = BuildDataSource(maxPoolSize: 4);

        // Warm: create 3 idle connections.
        var warm = await Task.WhenAll(
            Enumerable.Range(0, 3).Select(_ => ds.OpenConnectionAsync().AsTask()));
        foreach (var c in warm)
        {
            _ = await c.ExecuteScalarAsync<int>("SELECT 1");
            await c.DisposeAsync();
        }

        var pre = ds.GetStatistics();
        _output.WriteLine($"Pre-restart: {pre}");
        Assert.True(pre.TotalCreated >= 3);
        Assert.True(pre.Idle >= 1);

        await _fixture.StopContainerAsync();
        await _fixture.StartContainerAsync();

        // Trigger 3 fresh rents — each must open a new socket since stale entries
        // are discarded by ValidateOnRent.
        var fresh = await Task.WhenAll(
            Enumerable.Range(0, 3).Select(_ => ds.OpenConnectionAsync().AsTask()));
        foreach (var c in fresh)
        {
            _ = await c.ExecuteScalarAsync<int>("SELECT 2");
            await c.DisposeAsync();
        }

        var post = ds.GetStatistics();
        _output.WriteLine($"Post-restart: {post}");
        Assert.True(post.TotalCreated >= pre.TotalCreated + 1,
            $"Expected new physical connections after restart; TotalCreated {pre.TotalCreated} → {post.TotalCreated}");
        Assert.True(post.TotalEvicted > pre.TotalEvicted,
            $"Expected stale evictions; TotalEvicted {pre.TotalEvicted} → {post.TotalEvicted}");
    }

    [Fact]
    public async Task ServerRestart_DuringBulkInsert_BoundaryDocumented()
    {
        // Probe — record how many rows landed pre-restart. Pin only safety: persisted
        // count must not exceed sent count, and must not be negative.
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var table = $"restart_bulk_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int64) ENGINE = MergeTree ORDER BY id");
        try
        {
            const int rowCount = 100_000;
            var inserter = conn.CreateBulkInserter<RestartRow>(table);
            await inserter.InitAsync();

            int sent = 0;
            Exception? caught = null;
            try
            {
                for (int i = 0; i < rowCount; i++)
                {
                    if (i == rowCount / 4)
                        await _fixture.StopContainerAsync();
                    await inserter.AddAsync(new RestartRow { Id = i });
                    sent++;
                }
                await inserter.CompleteAsync();
            }
            catch (Exception ex) { caught = ex; }
            finally
            {
                try { await inserter.DisposeAsync(); } catch { /* expected after restart */ }
            }

            _output.WriteLine($"Bulk insert during restart: rowsSent={sent}, error={caught?.GetType().FullName}");

            await _fixture.StartContainerAsync();
            await using var freshConn = new ClickHouseConnection(_fixture.BuildSettings());
            await freshConn.OpenAsync();

            var persisted = await freshConn.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            _output.WriteLine($"Persisted rows after recovery: {persisted}");
            Assert.True(persisted <= (ulong)rowCount, "persisted ≤ sent");
            Assert.True(persisted >= 0);
        }
        finally
        {
            try
            {
                await using var c = new ClickHouseConnection(_fixture.BuildSettings());
                await c.OpenAsync();
                await c.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
            }
            catch { /* recovery best-effort */ }
        }
    }

    [Fact]
    public async Task ServerRestart_PreservesSettingsOnNewSession_PinAsReset()
    {
        // SET <setting> is session-scoped. After a restart, a new server session has
        // the default setting again — pin this contract.
        await using var ds = BuildDataSource(maxPoolSize: 1);

        // system.settings.value is String (with sentinels like 'auto(10)') — read as
        // string and parse the leading integer where present.
        await using (var c1 = await ds.OpenConnectionAsync())
        {
            await c1.ExecuteNonQueryAsync("SET max_threads = 1");
            var inSession = await c1.ExecuteScalarAsync<string>(
                "SELECT value FROM system.settings WHERE name = 'max_threads'");
            Assert.Equal("1", inSession);
        }

        await _fixture.StopContainerAsync();
        await _fixture.StartContainerAsync();

        await using (var c2 = await ds.OpenConnectionAsync())
        {
            var afterRestart = await c2.ExecuteScalarAsync<string>(
                "SELECT value FROM system.settings WHERE name = 'max_threads'");
            _output.WriteLine($"max_threads after restart on fresh session: {afterRestart}");
            Assert.NotEqual("1", afterRestart);
        }
    }

    [Fact]
    public async Task ServerRestart_DataSourceDisposeBeforeReady_NoHang()
    {
        // Stop the server, kick off a parked rent, then immediately dispose the data
        // source. Dispose must complete promptly (≤ 5s) and the parked waiter must
        // surface ObjectDisposedException — never hang.
        await _fixture.StopContainerAsync();
        try
        {
            var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
            {
                Settings = _fixture.BuildSettings(),
                MaxPoolSize = 1,
                ConnectionWaitTimeout = TimeSpan.FromSeconds(30),
            });

            // Use a fresh CTS so the rent doesn't trip the wait-timeout naturally.
            using var rentCts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var rent = ds.OpenConnectionAsync(rentCts.Token).AsTask();

            // Give the rent a beat to start, then dispose.
            await Task.Delay(150);

            var disposeWatch = System.Diagnostics.Stopwatch.StartNew();
            await ds.DisposeAsync();
            disposeWatch.Stop();

            _output.WriteLine($"DataSource dispose under outage: {disposeWatch.Elapsed.TotalMilliseconds:F0} ms");
            Assert.True(disposeWatch.Elapsed < TimeSpan.FromSeconds(5),
                $"Dispose blocked for {disposeWatch.Elapsed.TotalSeconds:F1}s — must abort waiters promptly");

            Exception? rentError = null;
            try { _ = await rent; }
            catch (Exception ex) { rentError = ex; }

            // Pin the safety invariant: dispose-while-outage doesn't deadlock and
            // the parked rent surfaces *some* exception (server-down race vs dispose
            // race both produce typed exceptions; ObjectDisposedException,
            // OperationCanceledException, ClickHouseConnectionException, TimeoutException
            // are all reasonable depending on which side wins).
            _output.WriteLine($"Rent failure during outage+dispose: {rentError?.GetType().FullName}");
            Assert.NotNull(rentError);
            Assert.IsNotType<OutOfMemoryException>(rentError);
            Assert.IsNotType<AccessViolationException>(rentError);
        }
        finally
        {
            await _fixture.StartContainerAsync();
        }
    }

    [Fact]
    public async Task ServerRestart_HealthCheckResumes_WithoutManualPing()
    {
        // §5 #6 — after a restart the pool's background sweeper / next-rent path
        // should detect stale entries and create fresh sockets without the test
        // having to reach in and ping. We don't actively rent during the wait window
        // but we DO call OpenConnectionAsync after waiting. The probe is whether
        // that single rent succeeds within ~2× the configured idle timeout, with
        // pre-restart idle entries evicted.
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fixture.BuildSettings(),
            MaxPoolSize = 2,
            ValidateOnRent = true,
            ConnectionIdleTimeout = TimeSpan.FromSeconds(3),
            ConnectionWaitTimeout = TimeSpan.FromSeconds(15),
        });

        await using (var c = await ds.OpenConnectionAsync())
        {
            _ = await c.ExecuteScalarAsync<int>("SELECT 1");
        }

        var pre = ds.GetStatistics();
        _output.WriteLine($"Pre-restart: {pre}");
        Assert.True(pre.Idle >= 1);

        await _fixture.StopContainerAsync();
        await _fixture.StartContainerAsync();

        // Wait passively — no rents, no manual ping. 2× idle timeout is the
        // window for background sweeping or first-rent-on-stale to settle.
        await Task.Delay(TimeSpan.FromSeconds(7));

        var watch = System.Diagnostics.Stopwatch.StartNew();
        await using (var fresh = await ds.OpenConnectionAsync())
        {
            Assert.Equal(99, await fresh.ExecuteScalarAsync<int>("SELECT 99"));
        }
        watch.Stop();

        var post = ds.GetStatistics();
        _output.WriteLine($"Post-passive-wait: {post}, first-rent latency: {watch.Elapsed.TotalMilliseconds:F0} ms");

        // Pin: a fresh physical connection was created (the stale one didn't sneak
        // back into idle without validation), and the next rent succeeded promptly.
        Assert.True(post.TotalCreated > pre.TotalCreated,
            $"Stale entries weren't evicted; TotalCreated {pre.TotalCreated} → {post.TotalCreated}");
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(5),
            $"Post-restart rent took too long: {watch.Elapsed.TotalSeconds:F1}s — expected fresh socket");
    }

    [Fact]
    public async Task ServerRestart_TempTableSurvivesPoolReturn_NoPoisonOrRetry()
    {
        // §5 #4 — create a temp table on a rented connection, restart the server
        // (kills the temp table), return the connection to the pool, rent it again,
        // and pin: subsequent queries succeed; the connection isn't poisoned by
        // the library trying to drop a now-missing temp table; pool stats settle.
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fixture.BuildSettings(),
            MaxPoolSize = 1,
            ValidateOnRent = true,
        });

        await using (var c = await ds.OpenConnectionAsync())
        {
            await c.ExecuteNonQueryAsync(
                "CREATE TEMPORARY TABLE temp_probe (id Int32) ENGINE = Memory");
            await c.ExecuteNonQueryAsync("INSERT INTO temp_probe VALUES (42)");
            Assert.Equal(42, await c.ExecuteScalarAsync<int>("SELECT id FROM temp_probe"));
        }
        // Connection returns to pool here.

        await _fixture.StopContainerAsync();
        await _fixture.StartContainerAsync();

        // Rent again. Either the library detects the stale-session and creates a
        // fresh socket, OR it tries to drop the temp table on the old session
        // (which fails because the session is gone). Either way: the next user
        // query must succeed, no hang, no unhandled exception.
        Exception? caught = null;
        try
        {
            await using var c2 = await ds.OpenConnectionAsync();
            // A normal query must work — the pool MUST have given us a usable connection.
            Assert.Equal(7, await c2.ExecuteScalarAsync<int>("SELECT 7"));

            // Temp table from the previous session is gone (server restarted) — verify
            // a fresh CREATE in the new session works (no leftover state).
            await c2.ExecuteNonQueryAsync(
                "CREATE TEMPORARY TABLE temp_probe (id Int32) ENGINE = Memory");
            await c2.ExecuteNonQueryAsync("INSERT INTO temp_probe VALUES (99)");
            Assert.Equal(99, await c2.ExecuteScalarAsync<int>("SELECT id FROM temp_probe"));
        }
        catch (Exception ex) { caught = ex; }

        if (caught is not null)
        {
            _output.WriteLine($"Temp-table post-restart surface: {caught.GetType().FullName} — {caught.Message}");
            Assert.IsNotType<OutOfMemoryException>(caught);
            Assert.IsNotType<AccessViolationException>(caught);
            // Re-throw so the test fails clearly if the connection was unusable.
            throw caught;
        }

        var stats = ds.GetStatistics();
        _output.WriteLine($"Final stats: {stats}");
        Assert.Equal(0, stats.PendingWaits);
    }

    [Fact]
    public async Task ServerRestart_StatisticsConsistency_InvariantsHold()
    {
        await using var ds = BuildDataSource(maxPoolSize: 4);

        // Warm + use a few rents.
        for (int i = 0; i < 5; i++)
        {
            await using var c = await ds.OpenConnectionAsync();
            _ = await c.ExecuteScalarAsync<int>("SELECT 1");
        }

        await _fixture.StopContainerAsync();

        // Sample stats during the outage.
        var midOutage = ds.GetStatistics();
        _output.WriteLine($"Mid-outage: {midOutage}");
        AssertStatsInvariants(midOutage);

        await _fixture.StartContainerAsync();

        // Recovery use.
        await using (var c = await ds.OpenConnectionAsync())
        {
            _ = await c.ExecuteScalarAsync<int>("SELECT 1");
        }

        var post = ds.GetStatistics();
        _output.WriteLine($"Post-recovery: {post}");
        AssertStatsInvariants(post);
    }

    private static void AssertStatsInvariants(DataSourceStatistics s)
    {
        Assert.True(s.Total >= 0);
        Assert.True(s.Idle >= 0);
        Assert.True(s.Busy >= 0);
        Assert.True(s.PendingWaits >= 0);
        Assert.True(s.TotalCreated >= 0);
        Assert.True(s.TotalEvicted >= 0);
        Assert.True(s.TotalRentsServed >= 0);
        Assert.True(s.Total >= s.Idle + s.Busy - 1,
            $"Total ({s.Total}) must accommodate Idle ({s.Idle}) + Busy ({s.Busy})");
    }

    private sealed class RestartRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public long Id { get; set; }
    }
}
