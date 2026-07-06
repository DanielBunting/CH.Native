using CH.Native.Connection;
using CH.Native.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace CH.Native.Tests.Unit.DependencyInjection;

/// <summary>
/// Covers the <c>AddClickHouse(IServiceCollection, ClickHouseConnectionSettings)</c> overload and the
/// SSH branches of its settings clone (key-path vs raw key bytes).
/// </summary>
public class ServiceCollectionSettingsOverloadTests
{
    private static ClickHouseConnectionSettings Settings(Action<ClickHouseConnectionSettingsBuilder> cfg)
    {
        var b = ClickHouseConnectionSettings.CreateBuilder().WithHost("h");
        cfg(b);
        return b.Build();
    }

    [Fact]
    public void AddClickHouse_Settings_SshKeyPath_ResolvesDataSource()
    {
        var services = new ServiceCollection();
        services.AddClickHouse(Settings(b => b.WithSshKeyPath("/k", "pw")));
        using var sp = services.BuildServiceProvider();
        Assert.NotNull(sp.GetRequiredService<ClickHouseDataSource>());
    }

    [Fact]
    public void AddClickHouse_Settings_SshKeyBytes_ResolvesDataSource()
    {
        var services = new ServiceCollection();
        services.AddClickHouse(Settings(b => b.WithSshKey(new byte[] { 1, 2, 3 }, "pw")));
        using var sp = services.BuildServiceProvider();
        Assert.NotNull(sp.GetRequiredService<ClickHouseDataSource>());
    }

    [Fact]
    public void AddClickHouse_NullServices_Throws() =>
        Assert.Throws<ArgumentNullException>(() =>
            ((IServiceCollection)null!).AddClickHouse(
                ClickHouseConnectionSettings.CreateBuilder().WithHost("h").Build()));

    [Fact]
    public void AddClickHouse_NullSettings_Throws() =>
        Assert.Throws<ArgumentNullException>(() =>
            new ServiceCollection().AddClickHouse((ClickHouseConnectionSettings)null!));
}
