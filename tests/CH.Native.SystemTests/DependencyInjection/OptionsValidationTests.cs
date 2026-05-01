using CH.Native.Connection;
using CH.Native.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace CH.Native.SystemTests.DependencyInjection;

/// <summary>
/// Pre-fix misconfigured <see cref="ClickHouseConnectionOptions"/> bound from
/// <c>appsettings.json</c> only surfaced at the first
/// <c>OpenConnectionAsync</c> as a generic
/// <see cref="ArgumentOutOfRangeException"/>. The validator now runs at the
/// time the snapshot is captured (i.e. at <c>AddClickHouse(IConfiguration)</c>
/// call), so every misconfig fails before the host wires up.
/// </summary>
[Trait(Categories.Name, Categories.DependencyInjection)]
public class OptionsValidationTests
{
    [Fact]
    public void InvalidPoolSize_FailsAtRegistration()
    {
        var cfg = BuildConfig(new()
        {
            ["ClickHouse:Host"] = "localhost",
            ["ClickHouse:Pool:MaxPoolSize"] = "0", // invalid: must be >= 1
        });

        var services = new ServiceCollection();
        var ex = Assert.Throws<ArgumentException>(
            () => services.AddClickHouse(cfg.GetSection("ClickHouse")));
        Assert.Contains("MaxPoolSize", ex.Message);
    }

    [Fact]
    public void MinGreaterThanMax_FailsAtRegistration()
    {
        var cfg = BuildConfig(new()
        {
            ["ClickHouse:Host"] = "localhost",
            ["ClickHouse:Pool:MinPoolSize"] = "10",
            ["ClickHouse:Pool:MaxPoolSize"] = "5",
        });

        var services = new ServiceCollection();
        var ex = Assert.Throws<ArgumentException>(
            () => services.AddClickHouse(cfg.GetSection("ClickHouse")));
        Assert.Contains("MinPoolSize", ex.Message);
    }

    [Fact]
    public void JwtAuthMethod_WithoutToken_FailsAtRegistration()
    {
        var cfg = BuildConfig(new()
        {
            ["ClickHouse:Host"] = "localhost",
            ["ClickHouse:AuthMethod"] = nameof(ClickHouseAuthMethod.Jwt),
            // No JwtToken
        });

        var services = new ServiceCollection();
        var ex = Assert.Throws<ArgumentException>(
            () => services.AddClickHouse(cfg.GetSection("ClickHouse")));
        Assert.Contains("Jwt", ex.Message);
        Assert.Contains("JwtToken", ex.Message);
    }

    [Fact]
    public void SshKeyAuthMethod_WithoutPath_FailsAtRegistration()
    {
        var cfg = BuildConfig(new()
        {
            ["ClickHouse:Host"] = "localhost",
            ["ClickHouse:AuthMethod"] = nameof(ClickHouseAuthMethod.SshKey),
        });

        var services = new ServiceCollection();
        var ex = Assert.Throws<ArgumentException>(
            () => services.AddClickHouse(cfg.GetSection("ClickHouse")));
        Assert.Contains("SshKey", ex.Message);
    }

    [Fact]
    public void OutOfRangePort_FailsAtRegistration()
    {
        var cfg = BuildConfig(new()
        {
            ["ClickHouse:Host"] = "localhost",
            ["ClickHouse:Port"] = "70000", // > 65535
        });

        var services = new ServiceCollection();
        var ex = Assert.Throws<ArgumentException>(
            () => services.AddClickHouse(cfg.GetSection("ClickHouse")));
        Assert.Contains("Port", ex.Message);
    }

    [Fact]
    public void ValidConfig_DoesNotThrow()
    {
        var cfg = BuildConfig(new()
        {
            ["ClickHouse:Host"] = "localhost",
            ["ClickHouse:Pool:MinPoolSize"] = "1",
            ["ClickHouse:Pool:MaxPoolSize"] = "10",
        });

        var services = new ServiceCollection();
        services.AddClickHouse(cfg.GetSection("ClickHouse"));
    }

    private static IConfigurationRoot BuildConfig(Dictionary<string, string?> values) =>
        new ConfigurationBuilder().AddInMemoryCollection(values).Build();
}
