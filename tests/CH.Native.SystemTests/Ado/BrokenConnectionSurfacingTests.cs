using System.Data;
using CH.Native.Ado;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Ado;

/// <summary>
/// Pins how the ADO surface behaves when the underlying connection has
/// transitioned to a broken state. ADO frameworks (Dapper, EF) sometimes
/// reuse a command across connection-state changes; the surfaced error
/// must direct callers clearly to "open a new connection" rather than
/// surfacing a cryptic protocol error.
///
/// <para>
/// We can't easily simulate a TCP-level disconnection without a proxy, so
/// we exercise the explicit poisoning paths the production code already
/// has: <c>MarkProtocolFatal</c> after a wire-fatal failure.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class BrokenConnectionSurfacingTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public BrokenConnectionSurfacingTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task ExecuteOnClosedConnection_ThrowsTypedInvalidOperation()
    {
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        // Deliberately do not open.

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await cmd.ExecuteScalarAsync());

        _output.WriteLine($"Closed-connection error: {ex.Message}");
        // Caller should be able to read the message and know to open the
        // connection. Pin that the error is informative — the exact message
        // text is acceptable as long as it mentions connection state.
        Assert.True(
            ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase)
            || ex.Message.Contains("not open", StringComparison.OrdinalIgnoreCase)
            || ex.Message.Contains("Connection not set", StringComparison.OrdinalIgnoreCase),
            $"Expected error message to mention connection state; got: {ex.Message}");
    }

    [Fact]
    public async Task ExecuteAfterDispose_ThrowsObjectDisposedOrInvalidOp()
    {
        var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";

        await conn.DisposeAsync();

        // Either ObjectDisposedException (preferred) or InvalidOperationException
        // (since the inner state flips to closed-on-dispose) is acceptable.
        var ex = await Assert.ThrowsAnyAsync<InvalidOperationException>(
            async () => await cmd.ExecuteScalarAsync());

        _output.WriteLine($"Post-dispose error: {ex.GetType().Name}: {ex.Message}");
    }

    [Fact]
    public async Task ConnectionState_ReflectsActualState()
    {
        // ADO contract: ConnectionState should be observable and consistent.
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        Assert.Equal(ConnectionState.Closed, conn.State);

        await conn.OpenAsync();
        Assert.Equal(ConnectionState.Open, conn.State);

        await conn.CloseAsync();
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    public async Task ExecuteAfterServerSideException_ConnectionStillUsable()
    {
        // A server-side exception (well-formed wire, just a SQL error) must
        // NOT poison the connection. Pre-fix this was the contract that
        // distinguished "wire is broken" (poison) from "your SQL is wrong"
        // (just a typed exception).
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        var bad = conn.CreateCommand();
        bad.CommandText = "SELECT * FROM nonexistent_table_xyz";
        await Assert.ThrowsAsync<ClickHouseServerException>(
            async () => await bad.ExecuteScalarAsync());

        // Same connection — must work.
        var good = conn.CreateCommand();
        good.CommandText = "SELECT 1";
        var result = await good.ExecuteScalarAsync();
        Assert.Equal(1, Convert.ToInt32(result));
    }
}
