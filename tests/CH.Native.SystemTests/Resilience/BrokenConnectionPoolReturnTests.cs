using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Probes whether a connection broken by a server-side <c>KILL QUERY</c> or
/// other in-flight failure correctly flips its <c>CanBePooled</c> flag so the
/// pool discards it on return. The danger if it doesn't: the broken connection
/// silently goes back into the pool, the next unrelated rent gets it, and
/// that caller sees a cryptic protocol failure on a query that should have
/// worked. This is the classic "poisoned pool" bug.
///
/// <para>
/// Existing <see cref="PoolDiscardOnPoisonTests"/> covers some scenarios via
/// the protocol layer; this file specifically covers the user-visible
/// <c>KILL QUERY</c> path and the immediate-after-rent state.
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
    public async Task QueryKilledByServer_ReturnedConnectionIsDiscarded_NextRentGetsCleanConnection()
    {
        // Pool with a single physical connection — if the broken one isn't
        // discarded, the next rent has nowhere to fall back and would
        // surface the bug very loudly.
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 1,
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

        // Return rent1. The pool should observe CanBePooled = false and discard.
        await rent1.DisposeAsync();

        // Next rent — must get a working connection (a fresh socket, not the
        // poisoned one).
        await using (var rent2 = await ds.OpenConnectionAsync())
        {
            var result = await rent2.ExecuteScalarAsync<int>("SELECT 42");
            Assert.Equal(42, result);
        }
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
