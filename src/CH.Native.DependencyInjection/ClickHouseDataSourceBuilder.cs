using System.Security.Cryptography.X509Certificates;
using CH.Native.Connection;
using Microsoft.Extensions.DependencyInjection;

namespace CH.Native.DependencyInjection;

internal sealed class ClickHouseDataSourceBuilder : IClickHouseDataSourceBuilder
{
    public ClickHouseDataSourceBuilder(IServiceCollection services, object? serviceKey)
    {
        Services = services;
        ServiceKey = serviceKey;
    }

    public IServiceCollection Services { get; }
    public object? ServiceKey { get; }

    // Provider registrations are captured here so the DataSource factory can
    // resolve them lazily at ClickHouseDataSource construction time.
    internal Func<IServiceProvider, Func<CancellationToken, ValueTask<string>>>? JwtProviderFactory { get; set; }
    internal Func<IServiceProvider, Func<CancellationToken, ValueTask<X509Certificate2>>>? CertificateProviderFactory { get; set; }
    internal Func<IServiceProvider, Func<CancellationToken, ValueTask<SshKeyMaterial>>>? SshKeyProviderFactory { get; set; }
    internal Func<IServiceProvider, Func<CancellationToken, ValueTask<string>>>? PasswordProviderFactory { get; set; }
    internal Action<ClickHouseDataSourceOptions>? PoolConfigurator { get; set; }

    public IClickHouseDataSourceBuilder WithJwtProvider<TProvider>()
        where TProvider : class, IClickHouseJwtProvider
    {
        RegisterKeyed<IClickHouseJwtProvider, TProvider>();
        JwtProviderFactory = sp =>
        {
            var provider = ResolveKeyed<IClickHouseJwtProvider>(sp);
            return ct => provider.GetTokenAsync(ct);
        };
        return this;
    }

    public IClickHouseDataSourceBuilder WithJwtProvider(
        Func<IServiceProvider, Func<CancellationToken, ValueTask<string>>> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        JwtProviderFactory = factory;
        return this;
    }

    public IClickHouseDataSourceBuilder WithCertificateProvider<TProvider>()
        where TProvider : class, IClickHouseCertificateProvider
    {
        RegisterKeyed<IClickHouseCertificateProvider, TProvider>();
        CertificateProviderFactory = sp =>
        {
            var provider = ResolveKeyed<IClickHouseCertificateProvider>(sp);
            return ct => provider.GetCertificateAsync(ct);
        };
        return this;
    }

    public IClickHouseDataSourceBuilder WithCertificateProvider(
        Func<IServiceProvider, Func<CancellationToken, ValueTask<X509Certificate2>>> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        CertificateProviderFactory = factory;
        return this;
    }

    public IClickHouseDataSourceBuilder WithSshKeyProvider<TProvider>()
        where TProvider : class, IClickHouseSshKeyProvider
    {
        RegisterKeyed<IClickHouseSshKeyProvider, TProvider>();
        SshKeyProviderFactory = sp =>
        {
            var provider = ResolveKeyed<IClickHouseSshKeyProvider>(sp);
            return ct => provider.GetKeyAsync(ct);
        };
        return this;
    }

    public IClickHouseDataSourceBuilder WithSshKeyProvider(
        Func<IServiceProvider, Func<CancellationToken, ValueTask<SshKeyMaterial>>> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        SshKeyProviderFactory = factory;
        return this;
    }

    public IClickHouseDataSourceBuilder WithPasswordProvider<TProvider>()
        where TProvider : class, IClickHousePasswordProvider
    {
        RegisterKeyed<IClickHousePasswordProvider, TProvider>();
        PasswordProviderFactory = sp =>
        {
            var provider = ResolveKeyed<IClickHousePasswordProvider>(sp);
            return ct => provider.GetPasswordAsync(ct);
        };
        return this;
    }

    public IClickHouseDataSourceBuilder WithPasswordProvider(
        Func<IServiceProvider, Func<CancellationToken, ValueTask<string>>> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        PasswordProviderFactory = factory;
        return this;
    }

    public IClickHouseDataSourceBuilder WithPool(Action<ClickHouseDataSourceOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        // Chain configurators so keyed DataSources can accumulate multiple pool hooks.
        var existing = PoolConfigurator;
        PoolConfigurator = existing is null
            ? configure
            : opts => { existing(opts); configure(opts); };
        return this;
    }

    public IClickHouseDataSourceBuilder ValidateOnStart()
    {
        // Multiple ValidateOnStart() calls on the same builder must register
        // only one hosted service, otherwise we'd resolve the DataSource N
        // times at startup. AddHostedService is keyless and Add (not TryAdd)
        // by default, so guard with a flag captured on the builder.
        if (_validateOnStartRegistered) return this;
        _validateOnStartRegistered = true;

        var serviceKey = ServiceKey;
        Services.AddHostedService(sp => new ClickHouseStartupValidator(sp, serviceKey));
        return this;
    }

    private bool _validateOnStartRegistered;

    private void RegisterKeyed<TService, TImpl>()
        where TService : class
        where TImpl : class, TService
    {
        if (ServiceKey is null)
            Services.AddSingleton<TService, TImpl>();
        else
            Services.AddKeyedSingleton<TService, TImpl>(ServiceKey);
    }

    private TService ResolveKeyed<TService>(IServiceProvider sp) where TService : notnull
    {
        return ServiceKey is null
            ? sp.GetRequiredService<TService>()
            : sp.GetRequiredKeyedService<TService>(ServiceKey);
    }
}
