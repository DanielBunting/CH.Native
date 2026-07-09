using CH.Native.Connection;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

/// <summary>
/// Pins that <see cref="ClickHouseConnectionSettings.ToString"/> never leaks secrets —
/// neither the password nor the JWT/bearer token appears in the string surface.
/// </summary>
public class SettingsSecretRedactionTests
{
    [Fact]
    public void ToString_OmitsPassword()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithUsername("default")
            .WithPassword("super-secret-password")
            .Build();

        Assert.DoesNotContain("super-secret-password", settings.ToString());
    }

    [Fact]
    public void ToString_OmitsJwtToken()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithJwt("jwt-secret-token-value")
            .Build();

        Assert.DoesNotContain("jwt-secret-token-value", settings.ToString());
    }
}
