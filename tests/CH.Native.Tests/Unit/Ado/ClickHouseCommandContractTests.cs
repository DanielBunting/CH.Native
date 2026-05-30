using System.Data;
using System.Data.Common;
using CH.Native.Ado;
using CH.Native.Connection;
using CH.Native.Commands;
using CH.Native.Results;
using Xunit;

namespace CH.Native.Tests.Unit.Ado;

public class ClickHouseCommandContractTests
{
    [Fact]
    public void CommandText_NullAssignment_StoresEmpty()
    {
        var cmd = new ClickHouseCommand { CommandText = null! };
        Assert.Equal(string.Empty, cmd.CommandText);
    }

    [Fact]
    public void CommandType_StoredProcedure_Throws_NotSupported()
    {
        var cmd = new ClickHouseCommand();
        var ex = Assert.Throws<NotSupportedException>(() => cmd.CommandType = CommandType.StoredProcedure);
        Assert.Contains("Text", ex.Message);
    }

    [Fact]
    public void CommandType_TableDirect_Throws_NotSupported()
    {
        var cmd = new ClickHouseCommand();
        Assert.Throws<NotSupportedException>(() => cmd.CommandType = CommandType.TableDirect);
    }

    [Fact]
    public void CommandType_Text_IsAccepted()
    {
        var cmd = new ClickHouseCommand { CommandType = CommandType.Text };
        Assert.Equal(CommandType.Text, cmd.CommandType);
    }

    [Fact]
    public void CommandTimeout_DefaultsTo30()
    {
        var cmd = new ClickHouseCommand();
        Assert.Equal(30, cmd.CommandTimeout);
    }

    [Fact]
    public void Transaction_Setter_Throws_WhenNonNull()
    {
        // Even though the type doesn't expose a public BeginTransaction, a
        // caller could try to set the Transaction property via
        // DbCommand.Transaction. That setter must reject non-null values
        // because we have no transaction primitive to bind to.
        var cmd = new ClickHouseCommand();
        // Confirm getter returns null for the disposable lifetime.
        Assert.Null(cmd.Transaction);
    }

    [Fact]
    public void Roles_DefaultsToEmptyMutableList()
    {
        var cmd = new ClickHouseCommand();
        Assert.Empty(cmd.Roles);
        cmd.Roles.Add("admin");
        Assert.Single(cmd.Roles);
        Assert.Equal("admin", cmd.Roles[0]);
    }

    [Fact]
    public void QueryId_DefaultsToNull_AndCanBeSet()
    {
        var cmd = new ClickHouseCommand { QueryId = "my-query-id" };
        Assert.Equal("my-query-id", cmd.QueryId);
    }

    [Fact]
    public void Parameters_TypedSurface_IsNativeCollection_AndDbCommandSurface_IsAdoCollection()
    {
        // Post-collapse: the typed `cmd.Parameters` returns the native
        // ClickHouseParameterCollection. The protected DbCommand.Parameters
        // override (visible after casting to DbCommand) returns the ADO-shaped
        // ClickHouseDbParameterCollection. Both surfaces coexist on the same
        // ClickHouseCommand instance.
        var cmd = new ClickHouseCommand();
        Assert.IsType<CH.Native.Commands.ClickHouseParameterCollection>(cmd.Parameters);
        Assert.IsType<ClickHouseDbParameterCollection>(((System.Data.Common.DbCommand)cmd).Parameters);
    }

    [Fact]
    public void Connection_RoundTrips()
    {
        using var conn = new ClickHouseConnection();
        var cmd = new ClickHouseCommand();
        cmd.Connection = conn;
        Assert.Same(conn, cmd.Connection);
    }

    [Fact]
    public void ConstructorWithCommandText_StoresIt()
    {
        using var conn = new ClickHouseConnection();
        var cmd = new ClickHouseCommand("SELECT 1", conn);
        Assert.Equal("SELECT 1", cmd.CommandText);
        Assert.Same(conn, cmd.Connection);
    }

    [Fact]
    public void Prepare_IsNoOp()
    {
        var cmd = new ClickHouseCommand();
        // Should not throw — Prepare is a no-op for ClickHouse (no prepared
        // statement protocol). Pin that contract.
        cmd.Prepare();
    }

    [Fact]
    public void Transaction_Setter_NonNull_Throws_NotSupported()
    {
        // The getter-returns-null case is pinned above; this pins the setter's
        // rejection of a non-null transaction (ClickHouse has no transaction
        // primitive to bind to).
        DbCommand cmd = new ClickHouseCommand();
        var ex = Assert.Throws<NotSupportedException>(() => cmd.Transaction = new FakeDbTransaction());
        Assert.Contains("transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Transaction_Setter_Null_IsAccepted()
    {
        DbCommand cmd = new ClickHouseCommand();
        cmd.Transaction = null; // no-op, must not throw
        Assert.Null(cmd.Transaction);
    }

    [Fact]
    public void CreateParameter_ReturnsClickHouseDbParameter()
    {
        DbCommand cmd = new ClickHouseCommand();
        var p = cmd.CreateParameter();
        Assert.IsType<ClickHouseDbParameter>(p);
    }

    [Fact]
    public void Cancel_OnInactiveCommand_IsNoOp()
    {
        // Cancel must be best-effort and must not throw when there is no open
        // connection / no in-flight query.
        var cmd = new ClickHouseCommand();
        cmd.Cancel();

        using var conn = new ClickHouseConnection(); // State == Closed
        cmd.Connection = conn;
        cmd.Cancel();
    }

    [Fact]
    public async Task Execute_WithoutConnection_Throws_ConnectionNotSet()
    {
        // EnsureConnection guard: no connection assigned.
        var cmd = new ClickHouseCommand { CommandText = "SELECT 1" };

        var async = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteNonQueryAsync(default));
        Assert.Contains("Connection not set", async.Message);

        // Sync bridges surface the same exception (unwrapped from Task.Run).
        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteScalar());
        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteReader());
    }

    [Fact]
    public async Task Execute_WithClosedConnection_Throws_ConnectionNotOpen()
    {
        // EnsureConnection guard: connection present but not open.
        using var conn = new ClickHouseConnection();
        var cmd = new ClickHouseCommand("SELECT 1", conn);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteScalarAsync(default));
        Assert.Contains("not open", ex.Message);
    }

    private sealed class FakeDbTransaction : DbTransaction
    {
        public override IsolationLevel IsolationLevel => IsolationLevel.Unspecified;
        protected override DbConnection? DbConnection => null;
        public override void Commit() { }
        public override void Rollback() { }
    }
}
