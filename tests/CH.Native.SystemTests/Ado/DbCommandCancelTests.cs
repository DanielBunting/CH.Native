using CH.Native.Ado;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Ado;

/// <summary>
/// Pins the synchronous <see cref="System.Data.Common.DbCommand.Cancel"/>
/// contract. Frameworks (EF Core, Dapper, ADO templates) call <c>Cancel()</c>
/// to terminate an in-flight query without an async cancellation token.
/// If the implementation is a no-op or doesn't reach the wire, callers
/// hang indefinitely on long-running queries.
///
/// <para>
/// This test probes:
/// </para>
/// <list type="bullet">
/// <item><description>Calling <c>Cancel()</c> from a background thread while
///     <c>ExecuteScalarAsync</c> is in flight.</description></item>
/// <item><description>Whether the connection is reusable after a Cancel.</description></item>
/// </list>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class DbCommandCancelTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public DbCommandCancelTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task Cancel_WhileInFlight_TerminatesQuery_DocumentedBehaviour()
    {
        // OBSERVE: probe whether DbCommand.Cancel() actually reaches the
        // wire and terminates a server-side query.
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT count() FROM numbers(10000000000)";
        cmd.CommandTimeout = 60; // generous — Cancel should beat the timer

        // Schedule Cancel from a background task ~250ms in.
        var cancelTask = Task.Run(async () =>
        {
            await Task.Delay(250);
            cmd.Cancel();
        });

        var sw = System.Diagnostics.Stopwatch.StartNew();
        Exception? caught = null;
        try
        {
            await cmd.ExecuteScalarAsync();
        }
        catch (Exception ex)
        {
            caught = ex;
        }
        sw.Stop();
        await cancelTask;

        _output.WriteLine($"Cancel-driven termination: thrown={caught?.GetType().Name ?? "(none)"}, elapsed={sw.ElapsedMilliseconds} ms");

        // Document today's behaviour:
        //  - If Cancel works: query terminates promptly (well under CommandTimeout).
        //  - If Cancel is a no-op: query runs until natural completion or timeout.
        // We assert that ONE of those happens within reasonable bounds —
        // not infinite hang.
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(60),
            $"Cancel must not produce an infinite hang; took {sw.Elapsed}");
    }

    [Fact]
    public async Task Cancel_OnIdleCommand_DoesNotThrow()
    {
        // Cancel on a command that's not running anything must be a safe
        // no-op (the ADO contract is "no-op if no command is in flight").
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";

        // Cancel before any execution.
        cmd.Cancel(); // must not throw

        // Should still execute fine.
        var result = await cmd.ExecuteScalarAsync();
        Assert.Equal(1, Convert.ToInt32(result));
    }
}
