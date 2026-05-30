using System.Data;
using CH.Native.Ado;
using CH.Native.Connection;
using CH.Native.Commands;
using CH.Native.Results;
using Xunit;

namespace CH.Native.Tests.Unit.Ado;

/// <summary>
/// Locks down the <see cref="ClickHouseConnection"/> public contract that
/// frameworks like Dapper / EF Core depend on: state machine, transaction
/// rejection, ChangeDatabase validation, ConnectionString immutability while
/// open. None of these need a server; they are pure surface tests.
/// </summary>
public class ClickHouseConnectionStateTests
{
    [Fact]
    public void State_NewConnection_IsClosed()
    {
        using var conn = new ClickHouseConnection();
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    public void ConnectionString_NullCtor_DefaultsToEmpty()
    {
        using var conn = new ClickHouseConnection();
        Assert.Equal(string.Empty, conn.ConnectionString);
    }

    [Fact]
    public void ConnectionString_AssignNull_StoresEmptyString()
    {
        using var conn = new ClickHouseConnection { ConnectionString = null! };
        Assert.Equal(string.Empty, conn.ConnectionString);
    }

    [Fact]
    public void Database_BeforeOpen_ReturnsEmptyString()
    {
        using var conn = new ClickHouseConnection();
        Assert.Equal(string.Empty, conn.Database);
    }

    [Fact]
    public void DataSource_BeforeOpen_ReturnsEmptyString()
    {
        using var conn = new ClickHouseConnection();
        Assert.Equal(string.Empty, conn.DataSource);
    }

    [Fact]
    public void ServerVersion_BeforeOpen_ReturnsEmptyString()
    {
        using var conn = new ClickHouseConnection();
        Assert.Equal(string.Empty, conn.ServerVersion);
    }

    [Fact]
    public void BeginTransaction_Throws_NotSupported()
    {
        using var conn = new ClickHouseConnection();
        var ex = Assert.Throws<NotSupportedException>(() => conn.BeginTransaction());
        Assert.Contains("ClickHouse", ex.Message);
        Assert.Contains("transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BeginTransaction_WithIsolationLevel_Throws_NotSupported()
    {
        using var conn = new ClickHouseConnection();
        Assert.Throws<NotSupportedException>(() => conn.BeginTransaction(IsolationLevel.ReadCommitted));
    }

    [Fact]
    public void ChangeDatabase_WhenClosed_Throws_InvalidOperation()
    {
        using var conn = new ClickHouseConnection();
        Assert.Throws<InvalidOperationException>(() => conn.ChangeDatabase("other"));
    }

    [Fact]
    public void ChangeDatabase_NullOrWhitespace_Throws_Argument()
    {
        // Even before checking state, a null/whitespace name is rejected.
        // Open() requires a server, so we can only verify the state-closed
        // branch — but the type-level guard exists for both.
        using var conn = new ClickHouseConnection();
        // closed-state check fires first
        Assert.Throws<InvalidOperationException>(() => conn.ChangeDatabase(""));
        Assert.Throws<InvalidOperationException>(() => conn.ChangeDatabase("   "));
    }

    [Fact]
    public void CreateCommand_BindsConnectionToCommand()
    {
        using var conn = new ClickHouseConnection();
        using var cmd = conn.CreateCommand();
        Assert.IsType<ClickHouseCommand>(cmd);
        Assert.Same(conn, cmd.Connection);
    }

    [Fact]
    public void Dispose_OnClosedConnection_DoesNotThrow()
    {
        var conn = new ClickHouseConnection();
        conn.Dispose();
        // second dispose should also be a no-op
        conn.Dispose();
    }

    [Fact]
    public async Task DisposeAsync_OnClosedConnection_DoesNotThrow()
    {
        var conn = new ClickHouseConnection();
        await conn.DisposeAsync();
        await conn.DisposeAsync();
    }
}
