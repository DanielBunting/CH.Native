using System.Security.Cryptography.X509Certificates;
using CH.Native.Connection;
using CH.Native.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace CH.Native.Tests.Unit.DependencyInjection;

/// <summary>
/// Covers the provider-registration builder methods that had no coverage: the factory-form
/// <c>WithCertificateProvider</c> and the generic <c>WithPasswordProvider&lt;T&gt;</c>.
/// </summary>
public class DataSourceBuilderProviderTests
{
    private sealed class FakePasswordProvider : IClickHousePasswordProvider
    {
        public ValueTask<string> GetPasswordAsync(CancellationToken cancellationToken) => new("pw");
    }

    private static ClickHouseDataSourceBuilder NewBuilder() => new(new ServiceCollection(), null);

    [Fact]
    public void WithCertificateProvider_Factory_SetsFactory()
    {
        var b = NewBuilder();
        var ret = b.WithCertificateProvider(sp => ct => ValueTask.FromResult<X509Certificate2>(null!));
        Assert.Same(b, ret);
        Assert.NotNull(b.CertificateProviderFactory);
    }

    [Fact]
    public void WithCertificateProvider_NullFactory_Throws()
    {
        var b = NewBuilder();
        Assert.Throws<ArgumentNullException>(() =>
            b.WithCertificateProvider((Func<IServiceProvider, Func<CancellationToken, ValueTask<X509Certificate2>>>)null!));
    }

    [Fact]
    public void WithPasswordProvider_Generic_SetsFactory()
    {
        var b = NewBuilder();
        var ret = b.WithPasswordProvider<FakePasswordProvider>();
        Assert.Same(b, ret);
        Assert.NotNull(b.PasswordProviderFactory);
    }

    [Fact]
    public void WithPool_Twice_ChainsConfigurators()
    {
        // First call: PoolConfigurator is null (assigned directly). Second call: non-null (chained) —
        // covers both arms of the null-coalescing branch.
        var b = NewBuilder();
        var order = new List<int>();
        b.WithPool(_ => order.Add(1));
        b.WithPool(_ => order.Add(2));
        Assert.NotNull(b.PoolConfigurator);
        b.PoolConfigurator!(new ClickHouseDataSourceOptions
        {
            Settings = ClickHouseConnectionSettings.CreateBuilder().WithHost("h").Build(),
        });
        Assert.Equal(new[] { 1, 2 }, order);
    }
}
