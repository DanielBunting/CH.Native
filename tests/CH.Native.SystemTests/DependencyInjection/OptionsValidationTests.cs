using CH.Native.Connection;
using CH.Native.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace CH.Native.SystemTests.DependencyInjection;

/// <summary>
/// The DI-layer validator is split: shape errors (pool bounds, port ranges)
/// fail fast at <c>AddClickHouse(IConfiguration)</c> registration time;
/// auth/credential pairing failures surface at first
/// <see cref="ClickHouseDataSource"/> resolution, after the user has had a
/// chance to chain <c>WithJwtProvider&lt;&gt;()</c> /
/// <c>WithSshKeyProvider&lt;&gt;()</c> on the returned builder. Apps that
/// want auth-pairing failures to surface at host startup opt in with
/// <see cref="IClickHouseDataSourceBuilder.ValidateOnStart"/>.
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
    public void JwtAuthMethod_WithoutToken_DoesNotFailAtRegistration()
    {
        // Pre-fix this threw at AddClickHouse — but a chained
        // .WithJwtProvider<>() call (which the user must be able to do
        // immediately after) was a false positive. Validation now defers
        // to first resolution.
        var cfg = BuildConfig(new()
        {
            ["ClickHouse:Host"] = "localhost",
            ["ClickHouse:AuthMethod"] = nameof(ClickHouseAuthMethod.Jwt),
        });

        var services = new ServiceCollection();
        services.AddClickHouse(cfg.GetSection("ClickHouse"));  // does not throw
    }

    [Fact]
    public void JwtAuthMethod_WithoutTokenAndWithoutProvider_FailsAtFirstResolution()
    {
        var cfg = BuildConfig(new()
        {
            ["ClickHouse:Host"] = "localhost",
            ["ClickHouse:AuthMethod"] = nameof(ClickHouseAuthMethod.Jwt),
        });

        var services = new ServiceCollection();
        services.AddClickHouse(cfg.GetSection("ClickHouse"));

        using var sp = services.BuildServiceProvider();
        var ex = Assert.Throws<ArgumentException>(() => sp.GetRequiredService<ClickHouseDataSource>());
        Assert.Contains("Jwt", ex.Message);
        Assert.Contains("JwtToken", ex.Message);
        Assert.Contains("ClickHouse", ex.Message); // section path prefix
    }

    [Fact]
    public async Task JwtAuthMethod_WithChainedProvider_ResolvesCleanly()
    {
        // The fix the whole exercise is about: chained provider
        // registration satisfies the auth pairing requirement.
        var cfg = BuildConfig(new()
        {
            ["ClickHouse:Host"] = "localhost",
            ["ClickHouse:AuthMethod"] = nameof(ClickHouseAuthMethod.Jwt),
        });

        var services = new ServiceCollection();
        services.AddClickHouse(cfg.GetSection("ClickHouse"))
            .WithJwtProvider<FakeJwtProvider>();

        await using var sp = services.BuildServiceProvider();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();
        Assert.NotNull(ds);
    }

    [Fact]
    public void SshKeyAuthMethod_WithoutPathAndWithoutProvider_FailsAtFirstResolution()
    {
        var cfg = BuildConfig(new()
        {
            ["ClickHouse:Host"] = "localhost",
            ["ClickHouse:AuthMethod"] = nameof(ClickHouseAuthMethod.SshKey),
        });

        var services = new ServiceCollection();
        services.AddClickHouse(cfg.GetSection("ClickHouse"));

        using var sp = services.BuildServiceProvider();
        var ex = Assert.Throws<ArgumentException>(() => sp.GetRequiredService<ClickHouseDataSource>());
        Assert.Contains("SshKey", ex.Message);
        Assert.Contains("SshPrivateKeyPath", ex.Message);
    }

    [Fact]
    public async Task SshKeyAuthMethod_WithChainedProvider_ResolvesCleanly()
    {
        var cfg = BuildConfig(new()
        {
            ["ClickHouse:Host"] = "localhost",
            ["ClickHouse:AuthMethod"] = nameof(ClickHouseAuthMethod.SshKey),
        });

        var services = new ServiceCollection();
        services.AddClickHouse(cfg.GetSection("ClickHouse"))
            .WithSshKeyProvider<FakeSshKeyProvider>();

        await using var sp = services.BuildServiceProvider();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();
        Assert.NotNull(ds);
    }

    [Fact]
    public async Task ValidateOnStart_WithoutProvider_ThrowsAtHostStart()
    {
        // Opt-in path that recovers the fail-fast guarantee for apps that
        // want misconfig surfaced at host startup, not first request.
        var cfg = BuildConfig(new()
        {
            ["ClickHouse:Host"] = "localhost",
            ["ClickHouse:AuthMethod"] = nameof(ClickHouseAuthMethod.Jwt),
        });

        var hostBuilder = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddClickHouse(cfg.GetSection("ClickHouse")).ValidateOnStart();
            });

        using var host = hostBuilder.Build();
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => host.StartAsync());
        Assert.Contains("Jwt", ex.Message);
    }

    [Fact]
    public async Task ValidateOnStart_WithChainedProvider_StartsCleanly()
    {
        var cfg = BuildConfig(new()
        {
            ["ClickHouse:Host"] = "localhost",
            ["ClickHouse:AuthMethod"] = nameof(ClickHouseAuthMethod.Jwt),
        });

        var hostBuilder = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddClickHouse(cfg.GetSection("ClickHouse"))
                    .WithJwtProvider<FakeJwtProvider>()
                    .ValidateOnStart();
            });

        using var host = hostBuilder.Build();
        await host.StartAsync();
        await host.StopAsync();
    }

    [Fact]
    public async Task ValidateOnStart_DoesNotDoubleResolve_WhenCalledMultipleTimes()
    {
        // Idempotency guard: chaining .ValidateOnStart() twice should not
        // register two hosted services (which would trigger two
        // GetRequiredService<ClickHouseDataSource>() calls at startup).
        // Validated indirectly: with a valid config we expect a single
        // host start to succeed and not throw on duplicate hosted services.
        var cfg = BuildConfig(new()
        {
            ["ClickHouse:Host"] = "localhost",
        });

        var hostBuilder = new HostBuilder()
            .ConfigureServices(services =>
            {
                var b = services.AddClickHouse(cfg.GetSection("ClickHouse"));
                b.ValidateOnStart();
                b.ValidateOnStart();
            });

        using var host = hostBuilder.Build();
        await host.StartAsync();
        await host.StopAsync();
    }

    [Fact]
    public void KeyedRegistration_PreservesDeferralAndSectionPath()
    {
        // Section path must propagate through to the deferred validator so
        // the consolidated message identifies the offending config block.
        var cfg = BuildConfig(new()
        {
            ["ClickHouse:Primary:Host"] = "localhost",
            ["ClickHouse:Primary:AuthMethod"] = nameof(ClickHouseAuthMethod.Jwt),
        });

        var services = new ServiceCollection();
        services.AddClickHouse("primary", cfg.GetSection("ClickHouse:Primary"));

        using var sp = services.BuildServiceProvider();
        var ex = Assert.Throws<ArgumentException>(
            () => sp.GetRequiredKeyedService<ClickHouseDataSource>("primary"));
        Assert.Contains("ClickHouse:Primary", ex.Message);
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

    private sealed class FakeJwtProvider : IClickHouseJwtProvider
    {
        public ValueTask<string> GetTokenAsync(CancellationToken ct) => new("test.jwt.token");
    }

    private sealed class FakeSshKeyProvider : IClickHouseSshKeyProvider
    {
        public ValueTask<SshKeyMaterial> GetKeyAsync(CancellationToken ct) =>
            new(new SshKeyMaterial(new byte[] { 0x00 }, Passphrase: null));
    }
}
