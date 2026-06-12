using CH.Native.Commands;
using CH.Native.Connection;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

/// <summary>
/// Every public/internal entry point that requires an open connection throws the
/// "Connection is not open." guard synchronously on a never-opened connection —
/// before any wire I/O, so no server is needed. (The _protocolFatal variant of the
/// same guard — "Connection is broken: …" — is pinned by
/// Integration.UnsupportedTypeFailureHardeningTests.)
/// </summary>
public class ConnectionNotOpenGuardTests
{
    private static ClickHouseConnection NewClosedConnection()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithUsername("default")
            .Build();
        return new ClickHouseConnection(settings);
    }

    private static void AssertNotOpen(Action action)
    {
        var ex = Assert.Throws<InvalidOperationException>(action);
        Assert.Equal("Connection is not open.", ex.Message);
    }

    private static async Task AssertNotOpenAsync(Func<Task> action)
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(action);
        Assert.Equal("Connection is not open.", ex.Message);
    }

    [Fact]
    public async Task ChangeDatabase_Throws()
    {
        await using var conn = NewClosedConnection();
        AssertNotOpen(() => conn.ChangeDatabase("other"));
    }

    [Fact]
    public async Task ChangeRolesAsync_Throws()
    {
        await using var conn = NewClosedConnection();
        await AssertNotOpenAsync(() => conn.ChangeRolesAsync(new[] { "role1" }));
    }

    [Fact]
    public async Task CreateBulkInserter_AllOverloads_Throw()
    {
        await using var conn = NewClosedConnection();
        AssertNotOpen(() => conn.CreateBulkInserter<DummyRow>("events"));
        AssertNotOpen(() => conn.CreateBulkInserter<DummyRow>("db", "events"));
        AssertNotOpen(() => conn.CreateBulkInserter("events", new[] { "col" }));
        AssertNotOpen(() => conn.CreateBulkInserter("db", "events", new[] { "col" }));
    }

    [Fact]
    public async Task ExecuteScalarAsync_Throws()
    {
        await using var conn = NewClosedConnection();
        await AssertNotOpenAsync(() => conn.ExecuteScalarAsync<int>("SELECT 1"));
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_Throws()
    {
        await using var conn = NewClosedConnection();
        await AssertNotOpenAsync(() => conn.ExecuteNonQueryAsync("SELECT 1"));
    }

    [Fact]
    public async Task ExecuteReaderAsync_Throws()
    {
        await using var conn = NewClosedConnection();
        await AssertNotOpenAsync(() => conn.ExecuteReaderAsync("SELECT 1"));
    }

    // QueryTypedAsync is an iterator — the guard fires on first MoveNextAsync.
    [Fact]
    public async Task QueryTypedAsync_Throws()
    {
        await using var conn = NewClosedConnection();
        var enumerator = conn.QueryTypedAsync<DummyRow>("SELECT 1").GetAsyncEnumerator();
        await AssertNotOpenAsync(async () => await enumerator.MoveNextAsync());
        await enumerator.DisposeAsync();
    }

    [Fact]
    public async Task ParameterizedPaths_Throw()
    {
        await using var conn = NewClosedConnection();
        var parameters = new ClickHouseParameterCollection();
        await AssertNotOpenAsync(() => conn.ExecuteScalarWithParametersAsync<int>("SELECT 1", parameters));
        await AssertNotOpenAsync(() => conn.ExecuteNonQueryWithParametersAsync("SELECT 1", parameters));
        await AssertNotOpenAsync(() => conn.ExecuteReaderWithParametersAsync("SELECT 1", parameters));
    }

    // The bulk-insert wire helpers carry their own guard because they bypass
    // SendQueryAsync.
    [Fact]
    public async Task BulkInsertSendPaths_Throw()
    {
        await using var conn = NewClosedConnection();
        await AssertNotOpenAsync(() => conn.SendInsertQueryAsync("INSERT INTO t VALUES", CancellationToken.None));
        await AssertNotOpenAsync(() => conn.SendDataBlockAsync(
            new[] { "col" }, new[] { "UInt8" }, new object?[][] { new object?[] { (byte)1 } }, 1, CancellationToken.None));
        await AssertNotOpenAsync(() => conn.SendEmptyBlockAsync(CancellationToken.None));
        await AssertNotOpenAsync(() => conn.SendDataBlockDirectAsync<DummyRow>(null!, null!, 0, CancellationToken.None));
    }

    private sealed class DummyRow
    {
        public long Value { get; set; }
    }
}
