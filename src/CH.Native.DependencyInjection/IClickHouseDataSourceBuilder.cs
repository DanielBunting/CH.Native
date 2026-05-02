using System.Security.Cryptography.X509Certificates;
using CH.Native.Connection;
using Microsoft.Extensions.DependencyInjection;

namespace CH.Native.DependencyInjection;

/// <summary>
/// Fluent handle returned by <see cref="ClickHouseServiceCollectionExtensions.AddClickHouse(IServiceCollection,string)"/>
/// and friends. Lets you attach credential providers (JWT, SSH key, mTLS cert,
/// password) and override pool options without peppering the registration
/// with <c>services.Configure</c> calls.
/// </summary>
public interface IClickHouseDataSourceBuilder
{
    /// <summary>The underlying <see cref="IServiceCollection"/>.</summary>
    IServiceCollection Services { get; }

    /// <summary>
    /// The keyed-service key this DataSource is registered under, or null when
    /// this is the unkeyed (default) registration.
    /// </summary>
    object? ServiceKey { get; }

    /// <summary>Registers a JWT provider that the pool invokes at connection-create time.</summary>
    IClickHouseDataSourceBuilder WithJwtProvider<TProvider>() where TProvider : class, IClickHouseJwtProvider;

    /// <summary>Registers a JWT provider via a delegate factory.</summary>
    IClickHouseDataSourceBuilder WithJwtProvider(Func<IServiceProvider, Func<CancellationToken, ValueTask<string>>> factory);

    /// <summary>Registers a certificate provider for mTLS.</summary>
    IClickHouseDataSourceBuilder WithCertificateProvider<TProvider>() where TProvider : class, IClickHouseCertificateProvider;

    /// <summary>Registers a certificate provider via a delegate factory.</summary>
    IClickHouseDataSourceBuilder WithCertificateProvider(Func<IServiceProvider, Func<CancellationToken, ValueTask<X509Certificate2>>> factory);

    /// <summary>Registers an SSH-key provider.</summary>
    IClickHouseDataSourceBuilder WithSshKeyProvider<TProvider>() where TProvider : class, IClickHouseSshKeyProvider;

    /// <summary>Registers an SSH-key provider via a delegate factory.</summary>
    IClickHouseDataSourceBuilder WithSshKeyProvider(Func<IServiceProvider, Func<CancellationToken, ValueTask<SshKeyMaterial>>> factory);

    /// <summary>Registers a password provider.</summary>
    IClickHouseDataSourceBuilder WithPasswordProvider<TProvider>() where TProvider : class, IClickHousePasswordProvider;

    /// <summary>Registers a password provider via a delegate factory.</summary>
    IClickHouseDataSourceBuilder WithPasswordProvider(Func<IServiceProvider, Func<CancellationToken, ValueTask<string>>> factory);

    /// <summary>Overrides pool options. Called after any config-bound defaults.</summary>
    IClickHouseDataSourceBuilder WithPool(Action<ClickHouseDataSourceOptions> configure);

    /// <summary>
    /// Forces the auth-pairing validator (and, by extension, the
    /// <see cref="ClickHouseDataSource"/> singleton factory) to run at host
    /// startup instead of at first resolution. Registers an internal
    /// <c>IHostedService</c>. Use this in apps that want a misconfigured
    /// <c>AuthMethod=Jwt</c> / <c>AuthMethod=SshKey</c> without a chained
    /// provider to fail at <c>app.Run()</c> rather than at the first request
    /// that injects the DataSource.
    /// </summary>
    IClickHouseDataSourceBuilder ValidateOnStart();
}
