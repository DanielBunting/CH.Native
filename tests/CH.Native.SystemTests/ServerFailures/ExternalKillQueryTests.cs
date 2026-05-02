using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.ServerFailures;

/// <summary>
/// Server-side <c>KILL QUERY</c> propagation: a long-running query on connection A
/// terminated by an external <c>KILL QUERY</c> issued from connection B. Mirrors the
/// real-world ops scenario where an admin kills a stuck query from a separate
/// session. Existing cancellation tests cover client-side cancellation; the
/// external-kill path is what production tools (DataGrip, etc.) actually trigger.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.ServerFailures)]
public class ExternalKillQueryTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ExternalKillQueryTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task ExternalKillQuery_TerminatesRunningQueryAndConnectionStaysUsable()
    {
        var queryId = Guid.NewGuid().ToString("D");

        await using var connA = new ClickHouseConnection(_fixture.BuildSettings());
        await connA.OpenAsync();

        // Long-running query on A. Use a query that is reliably killable: SELECT sleep
        // doesn't always honour KILL; SELECT count() FROM numbers(...) does because the
        // server checks cancellation between blocks.
        var queryTask = Task.Run(async () =>
        {
            return await connA.ExecuteScalarAsync<ulong>(
                "SELECT count() FROM numbers(10000000000)",
                queryId: queryId);
        });

        // Wait until the query is registered server-side, then kill from B.
        await using var connB = new ClickHouseConnection(_fixture.BuildSettings());
        await connB.OpenAsync();

        var registered = await WaitForQueryRegisteredAsync(connB, queryId, TimeSpan.FromSeconds(5));
        Assert.True(registered, $"Query {queryId} never registered in system.processes.");

        await connB.ExecuteNonQueryAsync($"KILL QUERY WHERE query_id = '{queryId}' SYNC");

        // A's task must surface an exception. The kill reason might come back as a
        // ClickHouseServerException (server-side error), or — depending on race —
        // as a connection-level exception. Either is acceptable; the contract is
        // "task fails, doesn't hang".
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => queryTask);
        _output.WriteLine($"A's task threw: {ex.GetType().Name}: {ex.Message}");

        // After the kill, A's connection must be reusable for a fresh query.
        var ok = await connA.ExecuteScalarAsync<int>("SELECT 1");
        Assert.Equal(1, ok);
    }

    [Fact]
    public async Task KillQueryAsync_HelperAlsoWorks()
    {
        // Same scenario but uses the public KillQueryAsync helper. Pins the contract
        // that the helper handles validation (Guid format), opens its own connection,
        // and executes the kill — i.e. the convenience path is on equal footing with
        // hand-written KILL QUERY SQL.
        var queryId = Guid.NewGuid().ToString("D");

        await using var connA = new ClickHouseConnection(_fixture.BuildSettings());
        await connA.OpenAsync();

        var queryTask = Task.Run(async () =>
        {
            return await connA.ExecuteScalarAsync<ulong>(
                "SELECT count() FROM numbers(10000000000)",
                queryId: queryId);
        });

        await using var connB = new ClickHouseConnection(_fixture.BuildSettings());
        await connB.OpenAsync();
        var registered = await WaitForQueryRegisteredAsync(connB, queryId, TimeSpan.FromSeconds(5));
        Assert.True(registered);

        await connB.KillQueryAsync(queryId);

        await Assert.ThrowsAnyAsync<Exception>(() => queryTask);

        Assert.Equal(1, await connA.ExecuteScalarAsync<int>("SELECT 1"));
    }

    [Fact]
    public async Task ExternalKill_PoolStatsConsistent()
    {
        // After an external kill of a rented connection, the pool must return to
        // a clean state — Busy back to 0, no leaked permits.
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fixture.BuildSettings(),
            MaxPoolSize = 4,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(10),
        });

        var queryId = Guid.NewGuid().ToString("D");
        var rented = await ds.OpenConnectionAsync();

        var queryTask = Task.Run(async () =>
        {
            try
            {
                return await rented.ExecuteScalarAsync<ulong>(
                    "SELECT count() FROM numbers(10000000000)",
                    queryId: queryId);
            }
            catch { return 0UL; }
        });

        await using (var killer = new ClickHouseConnection(_fixture.BuildSettings()))
        {
            await killer.OpenAsync();
            var registered = await WaitForQueryRegisteredAsync(killer, queryId, TimeSpan.FromSeconds(5));
            Assert.True(registered);
            await killer.ExecuteNonQueryAsync($"KILL QUERY WHERE query_id = '{queryId}' SYNC");
        }

        // Wait for the rented connection's task to surface the kill, then return
        // to the pool by disposing the rent.
        await queryTask;
        await rented.DisposeAsync();

        // Pool: nothing in flight, no pending waiters.
        var sw = Stopwatch.StartNew();
        var deadline = TimeSpan.FromSeconds(5);
        while (sw.Elapsed < deadline && ds.GetStatistics().Busy > 0)
            await Task.Delay(50);

        var stats = ds.GetStatistics();
        _output.WriteLine($"Pool after kill+return: Total={stats.Total}, Idle={stats.Idle}, Busy={stats.Busy}, PendingWaits={stats.PendingWaits}");
        Assert.Equal(0, stats.Busy);
        Assert.Equal(0, stats.PendingWaits);
    }

    private static async Task<bool> WaitForQueryRegisteredAsync(
        ClickHouseConnection probe, string queryId, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var n = await probe.ExecuteScalarAsync<ulong>(
                $"SELECT count() FROM system.processes WHERE query_id = '{queryId}'");
            if (n > 0) return true;
            await Task.Delay(50);
        }
        return false;
    }
}
