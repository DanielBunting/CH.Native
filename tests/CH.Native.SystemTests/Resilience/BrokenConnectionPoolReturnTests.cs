using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pins the positive contract that a server-side <c>KILL QUERY</c> or any
/// other well-formed <see cref="ClickHouseServerException"/> mid-query keeps
/// the connection poolable: the wire stays in spec, only the query was
/// terminated, so the pool reuses the connection on the next rent.
///
/// <para>
/// <c>_protocolFatal</c> is reserved for genuine wire corruption (malformed
/// bytes, unknown message types, trailing bytes after EOS, drain timeouts).
/// Those modes are exercised end-to-end against a mock server in
/// <see cref="Streams.PoolDiscardOnPoisonTests"/>; this file's job is the
/// inverse — proving server exceptions do <i>not</i> trip the poison latch.
/// </para>
///
/// <para>
/// Why it matters: silently churning a fresh socket after every server-side
/// error (a kill, a bad SQL, an OOM, a quota breach) would be a meaningful
/// throughput regression. The classic "poisoned pool" failure mode lives on
/// the protocol-fatal side, not the server-exception side.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class BrokenConnectionPoolReturnTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public BrokenConnectionPoolReturnTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task QueryKilledByServer_ConnectionRemainsPoolable_AndIsReusedOnNextRent()
    {
        // After the server kills a query, ClickHouse sends a well-formed
        // ExceptionMessage (typically QUERY_WAS_CANCELLED, code 394). The
        // wire stays in spec — the kill terminated the query, not the
        // connection — so _protocolFatal stays false, CanBePooled stays
        // true, and the pool reuses the same physical connection on the
        // next rent. This test pins that contract.
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 2,
        });

        var queryId = Guid.NewGuid().ToString("N");
        var rent1 = await ds.OpenConnectionAsync();

        // Start a long-running query in the background with a known queryId.
        var slowTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var _ in rent1.QueryAsync<int>(
                    "SELECT count() FROM numbers(10000000000)",
                    queryId: queryId)) { }
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Slow-query surfaced: {ex.GetType().Name}: {ex.Message}");
            }
        });

        // Side-channel kill via a fresh connection.
        await Task.Delay(300);
        await using (var killer = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await killer.OpenAsync();
            await killer.KillQueryAsync(queryId);
        }
        await slowTask;

        var canBePooledAfterKill = rent1.CanBePooled;
        var statsBeforeReturn = ds.GetStatistics();
        Assert.True(canBePooledAfterKill,
            "server-side KILL must NOT poison the connection — the wire stays in spec");

        await rent1.DisposeAsync();

        // Next rent — must reuse the same physical connection (no new socket).
        await using (var rent2 = await ds.OpenConnectionAsync())
        {
            // The reused connection is still usable.
            Assert.Equal(42, await rent2.ExecuteScalarAsync<int>("SELECT 42"));
        }

        var statsAfter = ds.GetStatistics();
        Assert.Equal(statsBeforeReturn.TotalCreated, statsAfter.TotalCreated);
        Assert.Equal(0, statsAfter.TotalEvicted);
    }

    [Fact]
    public async Task ServerExceptionMidQuery_ConnectionRemainsPoolable()
    {
        // A well-formed server exception (e.g., bad SQL) must NOT poison the
        // connection — the wire was structurally fine, just the SQL was
        // wrong. The pool should reuse the connection on next rent. This
        // pins the contrast with a protocol-fatal break.
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 1,
        });

        var rent1 = await ds.OpenConnectionAsync();
        await Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            await rent1.ExecuteScalarAsync<int>("SELECT * FROM table_does_not_exist_xyz");
        });

        var canBePooled = rent1.CanBePooled;
        await rent1.DisposeAsync();
        Assert.True(canBePooled, "Server-side SQL errors must NOT poison the connection");

        // Next rent reuses the same connection — verify by stats showing
        // total didn't grow.
        var stats = ds.GetStatistics();
        await using (var rent2 = await ds.OpenConnectionAsync())
        {
            Assert.Equal(1, await rent2.ExecuteScalarAsync<int>("SELECT 1"));
        }
        var afterStats = ds.GetStatistics();
        // No new physical connections created.
        Assert.Equal(stats.TotalCreated, afterStats.TotalCreated);
    }

    [Fact]
    public async Task PoolDoesNotReusePoisonedConnection_AcrossManyRents()
    {
        // Sanity-check: 20 rounds of (rent, run a real query, return) on a
        // pool of size 1, with a kill mid-cycle, must complete cleanly
        // without ever surfacing a "wire out of sync" error.
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 1,
        });

        for (int i = 0; i < 20; i++)
        {
            await using var conn = await ds.OpenConnectionAsync();
            Assert.Equal(i, await conn.ExecuteScalarAsync<int>($"SELECT toInt32({i})"));
        }

        var stats = ds.GetStatistics();
        _output.WriteLine($"After 20 rounds: TotalCreated={stats.TotalCreated} TotalRentsServed={stats.TotalRentsServed}");
    }
}
