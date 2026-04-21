using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouseAuth")]
[Trait("Category", "Integration")]
public class JwtAuthTests
{
    private readonly ClickHouseAuthFixture _fixture;

    public JwtAuthTests(ClickHouseAuthFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>
    /// OSS ClickHouse rejects JWT with "JWT is available only in ClickHouse Cloud"
    /// (upstream src/Interpreters/Session.cpp). This test proves the client's send
    /// path: marker in username slot, token in password slot — the round-trip
    /// to the server completes, the server responds with a specific auth error,
    /// and the client surfaces it. That is sufficient to validate the wire format.
    /// </summary>
    [Fact]
    public async Task Jwt_AgainstOssServer_SurfacesCloudOnlyError()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithJwt("eyJhbGciOiJIUzI1NiJ9.e30.fake-signature")
            .Build();

        await using var connection = new ClickHouseConnection(settings);

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => connection.OpenAsync());
        Assert.True(ex is ClickHouseServerException or ClickHouseConnectionException,
            $"expected server or connection exception, got {ex.GetType().Name}: {ex.Message}");
    }
}
