using CH.Native.Ado;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Ado;

/// <summary>
/// Pins the race semantics when both <see cref="System.Data.Common.DbCommand.CommandTimeout"/>
/// and an external <see cref="CancellationToken"/> can fire near the same
/// moment. Existing <c>CommandTimeoutTests</c> cover the pure-timeout case;
/// this covers the precedence and the post-failure connection survival.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class CommandTimeoutVsCancellationPrecedenceTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public CommandTimeoutVsCancellationPrecedenceTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task ExternalToken_FiresFirst_OperationCanceledExceptionWins()
    {
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        using var cmd = (ClickHouseDbCommand)conn.CreateCommand();
        cmd.CommandText = "SELECT count() FROM numbers(10000000000)";
        cmd.CommandTimeout = 30;

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => cmd.ExecuteScalarAsync(cts.Token));
        _output.WriteLine($"Cancellation surfaced: {ex.GetType().Name}");
    }

    [Fact]
    public async Task DisabledCommandTimeout_ZeroValue_ExternalTokenStillCancels()
    {
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        using var cmd = (ClickHouseDbCommand)conn.CreateCommand();
        cmd.CommandText = "SELECT count() FROM numbers(10000000000)";
        cmd.CommandTimeout = 0; // disabled

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => cmd.ExecuteScalarAsync(cts.Token));
    }

    [Fact]
    public async Task AfterTimeout_SubsequentCommands_OnSameConnection_Succeed()
    {
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        // First command times out (CommandTimeout in seconds).
        using (var slow = (ClickHouseDbCommand)conn.CreateCommand())
        {
            slow.CommandText = "SELECT count() FROM numbers(10000000000)";
            slow.CommandTimeout = 1;
            await Assert.ThrowsAnyAsync<Exception>(() => slow.ExecuteScalarAsync());
        }

        // Subsequent command on the same connection must succeed — the
        // timeout did not poison the wire.
        using (var ok = (ClickHouseDbCommand)conn.CreateCommand())
        {
            ok.CommandText = "SELECT 42";
            var result = await ok.ExecuteScalarAsync();
            Assert.Equal(42, Convert.ToInt32(result));
        }
    }
}
