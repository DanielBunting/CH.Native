using System.Security.Cryptography;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouseAuth")]
[Trait("Category", "Integration")]
public class SshAuthTests
{
    private readonly ClickHouseAuthFixture _fixture;

    public SshAuthTests(ClickHouseAuthFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task SshKey_Valid_ConnectsAndSelectsCurrentUser()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithUsername("ssh_user")
            .WithSshKey(_fixture.SshPrivateKeyPem)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        var principal = await connection.ExecuteScalarAsync<string>("SELECT currentUser()");
        Assert.Equal("ssh_user", principal);
    }

    [Fact]
    public async Task SshKey_Valid_RunsTrivialQuery()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithUsername("ssh_user")
            .WithSshKey(_fixture.SshPrivateKeyPem)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        var one = await connection.ExecuteScalarAsync<int>("SELECT 1");
        Assert.Equal(1, one);
    }

    [Fact]
    public async Task SshKey_WrongKey_FailsWithAuthException()
    {
        // Fresh RSA key — not the one configured for ssh_user on the server.
        using var wrongRsa = RSA.Create(2048);
        var wrongPem = System.Text.Encoding.UTF8.GetBytes(wrongRsa.ExportPkcs8PrivateKeyPem());

        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithUsername("ssh_user")
            .WithSshKey(wrongPem)
            .Build();

        await using var connection = new ClickHouseConnection(settings);

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => connection.OpenAsync());
        Assert.True(ex is ClickHouseServerException or ClickHouseConnectionException,
            $"expected auth failure, got {ex.GetType().Name}: {ex.Message}");
    }
}
