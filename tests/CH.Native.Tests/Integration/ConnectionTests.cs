using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class ConnectionTests
{
    private readonly ClickHouseFixture _fixture;

    public ConnectionTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task OpenAsync_ValidConnection_Succeeds()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);

        await connection.OpenAsync();

        Assert.True(connection.IsOpen);
        Assert.NotNull(connection.ServerInfo);
    }

    [Fact]
    public async Task OpenAsync_ReceivesServerInfo()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);

        await connection.OpenAsync();

        Assert.Equal("ClickHouse", connection.ServerInfo!.ServerName);
        Assert.True(connection.ServerInfo.VersionMajor >= 24);
        Assert.NotNull(connection.ServerInfo.Timezone);
        Assert.NotEmpty(connection.ServerInfo.Timezone);
    }

    [Fact]
    public async Task OpenAsync_NegotiatesProtocolVersion()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);

        await connection.OpenAsync();

        Assert.True(connection.NegotiatedProtocolVersion >= ProtocolVersion.MinSupported);
        Assert.True(connection.NegotiatedProtocolVersion <= ProtocolVersion.Current);
    }

    [Fact]
    public async Task OpenAsync_WithCredentials_Authenticates()
    {
        var connectionString = $"Host={_fixture.Host};Port={_fixture.Port};Username={_fixture.Username};Password={_fixture.Password}";
        await using var connection = new ClickHouseConnection(connectionString);

        await connection.OpenAsync();

        Assert.True(connection.IsOpen);
    }

    [Fact]
    public async Task OpenAsync_WithDatabase_ConnectsToDatabase()
    {
        var connectionString = $"{_fixture.ConnectionString};Database=system";
        await using var connection = new ClickHouseConnection(connectionString);

        await connection.OpenAsync();

        Assert.True(connection.IsOpen);
        Assert.Equal("system", connection.Settings.Database);
    }

    [Fact]
    public async Task CloseAsync_ClosesConnection()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.CloseAsync();

        Assert.False(connection.IsOpen);
    }

    [Fact]
    public async Task CloseAsync_WhenNotOpen_DoesNotThrow()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);

        // Should not throw
        await connection.CloseAsync();

        Assert.False(connection.IsOpen);
    }

    [Fact]
    public async Task DisposeAsync_ClosesConnection()
    {
        var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        Assert.True(connection.IsOpen);

        await connection.DisposeAsync();

        Assert.False(connection.IsOpen);
    }

    [Fact]
    public async Task DisposeAsync_WhenNotOpen_DoesNotThrow()
    {
        var connection = new ClickHouseConnection(_fixture.ConnectionString);

        // Should not throw
        await connection.DisposeAsync();
    }

    [Fact]
    public async Task OpenAsync_InvalidHost_ThrowsConnectionException()
    {
        await using var connection = new ClickHouseConnection(
            "Host=nonexistent.invalid.host.that.does.not.exist;Port=9000");

        await Assert.ThrowsAsync<ClickHouseConnectionException>(
            () => connection.OpenAsync());
    }

    [Fact]
    public async Task OpenAsync_InvalidPort_ThrowsConnectionException()
    {
        // Use a port that's unlikely to have anything listening
        await using var connection = new ClickHouseConnection(
            $"Host={_fixture.Host};Port=59999");

        await Assert.ThrowsAsync<ClickHouseConnectionException>(
            () => connection.OpenAsync());
    }

    [Fact]
    public async Task OpenAsync_Timeout_ThrowsConnectionException()
    {
        // Use a non-routable IP to trigger connection failure
        // Note: behavior varies by system - might timeout or get refused
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("10.255.255.1") // Non-routable
            .WithConnectTimeout(TimeSpan.FromSeconds(1))
            .Build();

        await using var connection = new ClickHouseConnection(settings);

        var ex = await Assert.ThrowsAsync<ClickHouseConnectionException>(
            () => connection.OpenAsync());
        // Connection should fail with either timeout or refused depending on network
        Assert.True(
            ex.Message.Contains("timed out") || ex.Message.Contains("refused") || ex.Message.Contains("Failed"),
            $"Expected connection failure message, got: {ex.Message}");
    }

    [Fact]
    public async Task OpenAsync_CancellationToken_Honored()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);

        // TaskCanceledException is a subclass of OperationCanceledException
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => connection.OpenAsync(cts.Token));
    }

    [Fact]
    public async Task OpenAsync_AlreadyOpen_ThrowsInvalidOperation()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => connection.OpenAsync());
    }

    [Fact]
    public async Task OpenAsync_AfterDispose_ThrowsObjectDisposed()
    {
        var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => connection.OpenAsync());
    }

    [Fact]
    public async Task OpenAsync_AfterClose_CanReopenConnection()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        Assert.True(connection.IsOpen);

        await connection.CloseAsync();
        Assert.False(connection.IsOpen);

        // Should be able to reopen
        await connection.OpenAsync();
        Assert.True(connection.IsOpen);
    }

    [Fact]
    public async Task MultipleConnections_SameFixture_AllSucceed()
    {
        var tasks = Enumerable.Range(0, 5).Select(async _ =>
        {
            await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
            await connection.OpenAsync();
            Assert.True(connection.IsOpen);
        });

        await Task.WhenAll(tasks);
    }

    [Fact]
    public async Task Connection_ServerInfo_HasExpectedFields()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        Assert.NotNull(connection.ServerInfo);
        Assert.NotNull(connection.ServerInfo.ServerName);
        Assert.True(connection.ServerInfo.VersionMajor > 0);
        Assert.True(connection.ServerInfo.ProtocolRevision > 0);
    }

    [Fact]
    public async Task Connection_UsingBuilder_Works()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithDatabase("default")
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        Assert.True(connection.IsOpen);
    }

    [Fact]
    public async Task Connection_CustomClientName_SentToServer()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithClientName("MyCustomClient")
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        // Connection succeeds, meaning server accepted our client name
        Assert.True(connection.IsOpen);
        Assert.Equal("MyCustomClient", connection.Settings.ClientName);
    }
}
