using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using CH.Native.Connection;
using CH.Native.DependencyInjection;
using CH.Native.DependencyInjection.HealthChecks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace CH.Native.Tests.Unit.DependencyInjection;

/// <summary>
/// Covers the remaining reachable branch arms in the DI registration paths: the
/// <see cref="IConfiguration"/> overloads' empty/non-section arms, the pool-options build path, the
/// settings-clone client-cert / roles arms, and the keyed vs non-keyed startup-validator branch.
/// </summary>
public class DependencyInjectionBranchCoverageTests
{
    private static X509Certificate2 SelfSigned()
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=chnative-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
    }

    private static ClickHouseDataSource Resolve(IServiceCollection s) =>
        s.BuildServiceProvider().GetRequiredService<ClickHouseDataSource>();

    // 44/45: an empty root IConfiguration -> Get() returns null (=> new options) and
    // (section as IConfigurationSection) is null (=> null path).
    [Fact]
    public void AddClickHouse_EmptyRootConfig_NonKeyed()
    {
        var services = new ServiceCollection();
        services.AddClickHouse((IConfiguration)new ConfigurationBuilder().Build());
        Assert.NotNull(Resolve(services));
    }

    // 94/95: same, keyed overload.
    [Fact]
    public void AddClickHouse_EmptyRootConfig_Keyed()
    {
        var services = new ServiceCollection();
        services.AddClickHouse("k", (IConfiguration)new ConfigurationBuilder().Build());
        using var sp = services.BuildServiceProvider();
        Assert.NotNull(sp.GetRequiredKeyedService<ClickHouseDataSource>("k"));
    }

    // 311/314: a configurator is present -> BuildPoolOptions builds options rather than the early return.
    [Fact]
    public void WithPool_BuildsPoolOptions()
    {
        var services = new ServiceCollection();
        services.AddClickHouse(ClickHouseConnectionSettings.CreateBuilder().WithHost("h").Build())
                .WithPool(_ => { });
        Assert.NotNull(Resolve(services));
    }

    // 355: CloneViaBuilder — TlsClientCertificate not-null arm.
    [Fact]
    public void AddClickHouse_SettingsWithClientCertificate()
    {
        var services = new ServiceCollection();
        services.AddClickHouse(ClickHouseConnectionSettings.CreateBuilder().WithHost("h")
            .WithTlsClientCertificate(SelfSigned()).Build());
        Assert.NotNull(Resolve(services));
    }

    // 356: CloneViaBuilder — Roles present arm.
    [Fact]
    public void AddClickHouse_SettingsWithRoles()
    {
        var services = new ServiceCollection();
        services.AddClickHouse(ClickHouseConnectionSettings.CreateBuilder().WithHost("h")
            .WithRoles(new[] { "r" }).Build());
        Assert.NotNull(Resolve(services));
    }

    // StartupValidator:28 — non-keyed arm.
    [Fact]
    public async Task ValidateOnStart_NonKeyed_ResolvesDataSource()
    {
        var services = new ServiceCollection();
        services.AddClickHouse("Host=h").ValidateOnStart();
        using var sp = services.BuildServiceProvider();
        foreach (var hs in sp.GetServices<IHostedService>())
            await hs.StartAsync(default);
    }

    // StartupValidator:28 — keyed arm.
    [Fact]
    public async Task ValidateOnStart_Keyed_ResolvesKeyedDataSource()
    {
        var services = new ServiceCollection();
        services.AddClickHouse("mykey", "Host=h").ValidateOnStart();
        using var sp = services.BuildServiceProvider();
        foreach (var hs in sp.GetServices<IHostedService>())
            await hs.StartAsync(default);
    }

    // 95 (section arm) + 314 (POCO pool present): a keyed real config section with a Pool sub-section.
    [Fact]
    public void AddClickHouse_KeyedConfigSection_WithPool()
    {
        // Bind every pool field so each `pocoPool?.X ?? defaults.X` arm takes its POCO-present side.
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ch:Host"] = "h",
            ["ch:Pool:MaxPoolSize"] = "5",
            ["ch:Pool:MinPoolSize"] = "1",
            ["ch:Pool:ConnectionIdleTimeout"] = "00:01:00",
            ["ch:Pool:ConnectionLifetime"] = "00:05:00",
            ["ch:Pool:ConnectionWaitTimeout"] = "00:00:30",
            ["ch:Pool:ValidateOnRent"] = "true",
            ["ch:Pool:PrewarmOnStart"] = "false",
        }).Build();
        var services = new ServiceCollection();
        services.AddClickHouse("k2", config.GetSection("ch"));
        using var sp = services.BuildServiceProvider();
        Assert.NotNull(sp.GetRequiredKeyedService<ClickHouseDataSource>("k2"));
    }

    // 352/353/354/357: CloneViaBuilder — UseTls / AllowInsecureTls / CaCert / Jwt present arms.
    [Fact]
    public void AddClickHouse_SettingsWithTlsAndJwt()
    {
        var s = ClickHouseConnectionSettings.CreateBuilder().WithHost("h")
            .WithTls().WithAllowInsecureTls().WithTlsCaCertificate("/ca").WithJwt("t").Build();
        var services = new ServiceCollection();
        services.AddClickHouse(s);
        Assert.NotNull(Resolve(services));
    }

    // HealthCheck:19 — the non-null data source arm.
    [Fact]
    public void HealthCheck_WithDataSource_Constructs()
    {
        var services = new ServiceCollection();
        services.AddClickHouse(ClickHouseConnectionSettings.CreateBuilder().WithHost("h").Build());
        var ds = Resolve(services);
        Assert.NotNull(new ClickHouseHealthCheck(ds, TimeSpan.FromSeconds(1)));
    }
}
