using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Integration tests for TLS/SSL connections to ClickHouse.
/// </summary>
[Collection("ClickHouseTls")]
public class TlsConnectionTests
{
    private readonly ClickHouseTlsFixture _fixture;

    public TlsConnectionTests(ClickHouseTlsFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task OpenAsync_WithTls_Succeeds()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);

        await connection.OpenAsync();

        Assert.True(connection.IsOpen);
        Assert.NotNull(connection.ServerInfo);
    }

    [Fact]
    public async Task OpenAsync_WithTls_CanExecuteQuery()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<int>("SELECT 1");

        Assert.Equal(1, result);
    }

    [Fact]
    public async Task OpenAsync_WithTlsBuilder_Succeeds()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithTls()
            .WithTlsPort(_fixture.TlsPort)
            .WithAllowInsecureTls() // Self-signed cert
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        Assert.True(connection.IsOpen);
        Assert.True(connection.Settings.UseTls);
    }

    [Fact]
    public async Task OpenAsync_WithTls_UsesCorrectPort()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithTls()
            .WithTlsPort(_fixture.TlsPort)
            .WithAllowInsecureTls()
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        Assert.Equal(_fixture.TlsPort, settings.EffectivePort);

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        Assert.True(connection.IsOpen);
    }

    [Fact]
    public async Task OpenAsync_TlsWithoutInsecure_FailsOnSelfSigned()
    {
        // Without AllowInsecureTls, should fail on self-signed cert
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithTls()
            .WithTlsPort(_fixture.TlsPort)
            .WithAllowInsecureTls(false) // Don't allow insecure
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        await using var connection = new ClickHouseConnection(settings);

        // Should throw because certificate validation fails
        await Assert.ThrowsAsync<ClickHouseConnectionException>(
            () => connection.OpenAsync());
    }

    [Fact]
    public async Task OpenAsync_TlsWithCredentials_Authenticates()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithTls()
            .WithTlsPort(_fixture.TlsPort)
            .WithAllowInsecureTls()
            .WithCredentials(_fixture.Username, _fixture.Password)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        // Query current user to verify authentication
        var currentUser = await connection.ExecuteScalarAsync<string>("SELECT currentUser()");

        Assert.Equal(_fixture.Username, currentUser);
    }

    [Fact]
    public async Task OpenAsync_TlsInvalidCredentials_Fails()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithTls()
            .WithTlsPort(_fixture.TlsPort)
            .WithAllowInsecureTls()
            .WithCredentials("invalid_user", "wrong_password")
            .Build();

        await using var connection = new ClickHouseConnection(settings);

        await Assert.ThrowsAsync<ClickHouseConnectionException>(
            () => connection.OpenAsync());
    }

    [Fact]
    public async Task ExecuteScalar_OverTls_ReturnsCorrectResult()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<long>("SELECT 12345678901234");

        Assert.Equal(12345678901234L, result);
    }

    [Fact]
    public async Task ExecuteNonQuery_OverTls_Succeeds()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Create and drop a table over TLS
        await connection.ExecuteNonQueryAsync(
            "CREATE TABLE IF NOT EXISTS test_tls (id UInt32) ENGINE = Memory");

        await connection.ExecuteNonQueryAsync("DROP TABLE IF EXISTS test_tls");
    }

    [Fact]
    public async Task Query_OverTls_StreamsResults()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var count = 0;
        await foreach (var row in connection.QueryAsync("SELECT number FROM system.numbers LIMIT 10"))
        {
            count++;
        }

        Assert.Equal(10, count);
    }

    [Fact]
    public async Task MultipleQueries_OverTls_AllSucceed()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Execute multiple queries over the same TLS connection
        for (int i = 0; i < 5; i++)
        {
            var result = await connection.ExecuteScalarAsync<int>($"SELECT {i}");
            Assert.Equal(i, result);
        }
    }

    [Fact]
    public async Task Connection_TlsServerInfo_HasExpectedFields()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        Assert.NotNull(connection.ServerInfo);
        Assert.Equal("ClickHouse", connection.ServerInfo.ServerName);
        Assert.True(connection.ServerInfo.VersionMajor >= 24);
        Assert.NotNull(connection.ServerInfo.Timezone);
    }
}
