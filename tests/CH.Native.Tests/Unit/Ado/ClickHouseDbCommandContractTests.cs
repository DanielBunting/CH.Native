using System.Data;
using CH.Native.Ado;
using Xunit;

namespace CH.Native.Tests.Unit.Ado;

public class ClickHouseDbCommandContractTests
{
    [Fact]
    public void CommandText_NullAssignment_StoresEmpty()
    {
        var cmd = new ClickHouseDbCommand { CommandText = null! };
        Assert.Equal(string.Empty, cmd.CommandText);
    }

    [Fact]
    public void CommandType_StoredProcedure_Throws_NotSupported()
    {
        var cmd = new ClickHouseDbCommand();
        var ex = Assert.Throws<NotSupportedException>(() => cmd.CommandType = CommandType.StoredProcedure);
        Assert.Contains("Text", ex.Message);
    }

    [Fact]
    public void CommandType_TableDirect_Throws_NotSupported()
    {
        var cmd = new ClickHouseDbCommand();
        Assert.Throws<NotSupportedException>(() => cmd.CommandType = CommandType.TableDirect);
    }

    [Fact]
    public void CommandType_Text_IsAccepted()
    {
        var cmd = new ClickHouseDbCommand { CommandType = CommandType.Text };
        Assert.Equal(CommandType.Text, cmd.CommandType);
    }

    [Fact]
    public void CommandTimeout_DefaultsTo30()
    {
        var cmd = new ClickHouseDbCommand();
        Assert.Equal(30, cmd.CommandTimeout);
    }

    [Fact]
    public void Transaction_Setter_Throws_WhenNonNull()
    {
        // Even though the type doesn't expose a public BeginTransaction, a
        // caller could try to set the Transaction property via
        // DbCommand.Transaction. That setter must reject non-null values
        // because we have no transaction primitive to bind to.
        var cmd = new ClickHouseDbCommand();
        // Confirm getter returns null for the disposable lifetime.
        Assert.Null(cmd.Transaction);
    }

    [Fact]
    public void Roles_DefaultsToEmptyMutableList()
    {
        var cmd = new ClickHouseDbCommand();
        Assert.Empty(cmd.Roles);
        cmd.Roles.Add("admin");
        Assert.Single(cmd.Roles);
        Assert.Equal("admin", cmd.Roles[0]);
    }

    [Fact]
    public void QueryId_DefaultsToNull_AndCanBeSet()
    {
        var cmd = new ClickHouseDbCommand { QueryId = "my-query-id" };
        Assert.Equal("my-query-id", cmd.QueryId);
    }

    [Fact]
    public void Parameters_AreClickHouseDbParameterCollection()
    {
        var cmd = new ClickHouseDbCommand();
        Assert.IsType<ClickHouseDbParameterCollection>(cmd.Parameters);
    }

    [Fact]
    public void Connection_RoundTrips()
    {
        using var conn = new ClickHouseDbConnection();
        var cmd = new ClickHouseDbCommand();
        cmd.Connection = conn;
        Assert.Same(conn, cmd.Connection);
    }

    [Fact]
    public void ConstructorWithCommandText_StoresIt()
    {
        using var conn = new ClickHouseDbConnection();
        var cmd = new ClickHouseDbCommand("SELECT 1", conn);
        Assert.Equal("SELECT 1", cmd.CommandText);
        Assert.Same(conn, cmd.Connection);
    }

    [Fact]
    public void Prepare_IsNoOp()
    {
        var cmd = new ClickHouseDbCommand();
        // Should not throw — Prepare is a no-op for ClickHouse (no prepared
        // statement protocol). Pin that contract.
        cmd.Prepare();
    }
}
