using CH.Native.Connection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace CH.Native.DependencyInjection;

/// <summary>
/// Registration helpers for CH.Native on <see cref="IServiceCollection"/>.
///
/// Registers <see cref="ClickHouseDataSource"/> as a singleton (or keyed
/// singleton when a service key is supplied). Consumers resolve the
/// DataSource and call <see cref="ClickHouseDataSource.OpenConnectionAsync"/>
/// per unit of work; the returned connection returns itself to the pool on
/// <c>DisposeAsync</c>.
///
/// The returned <see cref="IClickHouseDataSourceBuilder"/> lets callers chain
/// credential-provider registrations (JWT, SSH key, mTLS cert, password) and
/// pool overrides without losing type-safety.
/// </summary>
public static class ClickHouseServiceCollectionExtensions
{
    // -------- Unkeyed overloads --------

    /// <summary>Registers a DataSource built from a connection string.</summary>
    public static IClickHouseDataSourceBuilder AddClickHouse(this IServiceCollection services, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        return AddCore(services, serviceKey: null, sp => ClickHouseConnectionSettings.CreateBuilder()
            .WithConnectionString(connectionString));
    }

    /// <summary>Registers a DataSource built from an <see cref="IConfiguration"/> section.</summary>
    public static IClickHouseDataSourceBuilder AddClickHouse(this IServiceCollection services, IConfiguration section)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);
        var pocoSnapshot = section.Get<ClickHouseConnectionOptions>() ?? new ClickHouseConnectionOptions();
        return AddCore(services, serviceKey: null,
            builderFactory: _ => ClickHouseConnectionOptionsMapper.CreateBuilder(pocoSnapshot),
            pocoSnapshot: pocoSnapshot);
    }

    /// <summary>Registers a DataSource configured via a fluent builder.</summary>
    public static IClickHouseDataSourceBuilder AddClickHouse(
        this IServiceCollection services,
        Action<ClickHouseConnectionSettingsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        return AddCore(services, serviceKey: null, sp =>
        {
            var b = ClickHouseConnectionSettings.CreateBuilder();
            configure(b);
            return b;
        });
    }

    /// <summary>Registers a DataSource from pre-built settings.</summary>
    public static IClickHouseDataSourceBuilder AddClickHouse(this IServiceCollection services, ClickHouseConnectionSettings settings)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(settings);
        return AddCore(services, serviceKey: null, sp => CloneViaBuilder(settings));
    }

    // -------- Keyed overloads (match Aspire naming) --------

    /// <summary>Registers a keyed DataSource from a connection string.</summary>
    public static IClickHouseDataSourceBuilder AddClickHouse(this IServiceCollection services, string serviceKey, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        return AddCore(services, serviceKey, sp => ClickHouseConnectionSettings.CreateBuilder()
            .WithConnectionString(connectionString));
    }

    /// <summary>Registers a keyed DataSource from a configuration section.</summary>
    public static IClickHouseDataSourceBuilder AddClickHouse(this IServiceCollection services, string serviceKey, IConfiguration section)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceKey);
        ArgumentNullException.ThrowIfNull(section);
        var pocoSnapshot = section.Get<ClickHouseConnectionOptions>() ?? new ClickHouseConnectionOptions();
        return AddCore(services, serviceKey,
            builderFactory: _ => ClickHouseConnectionOptionsMapper.CreateBuilder(pocoSnapshot),
            pocoSnapshot: pocoSnapshot);
    }

    /// <summary>Registers a keyed DataSource configured via a fluent builder.</summary>
    public static IClickHouseDataSourceBuilder AddClickHouse(
        this IServiceCollection services,
        string serviceKey,
        Action<ClickHouseConnectionSettingsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceKey);
        ArgumentNullException.ThrowIfNull(configure);
        return AddCore(services, serviceKey, sp =>
        {
            var b = ClickHouseConnectionSettings.CreateBuilder();
            configure(b);
            return b;
        });
    }

    // -------- Core --------

    private static IClickHouseDataSourceBuilder AddCore(
        IServiceCollection services,
        object? serviceKey,
        Func<IServiceProvider, ClickHouseConnectionSettingsBuilder> builderFactory,
        ClickHouseConnectionOptions? pocoSnapshot = null)
    {
        var dsBuilder = new ClickHouseDataSourceBuilder(services, serviceKey);

        // DataSource registration. Lifetime: singleton (pool is shared).
        // Notably we do NOT register ClickHouseConnection or ClickHouseDbConnection
        // as transients — opening a connection is async, and sync-over-async in
        // a DI factory risks deadlocks. Consumers take the DataSource and call
        // OpenConnectionAsync() themselves. This matches Npgsql's guidance for
        // async-first workloads.
        if (serviceKey is null)
        {
            services.TryAddSingleton<ClickHouseDataSource>(sp => CreateDataSource(sp, dsBuilder, builderFactory, pocoSnapshot));
        }
        else
        {
            services.TryAddKeyedSingleton<ClickHouseDataSource>(serviceKey,
                (sp, key) => CreateDataSource(sp, dsBuilder, builderFactory, pocoSnapshot));
        }

        return dsBuilder;
    }

    private static ClickHouseDataSource CreateDataSource(
        IServiceProvider sp,
        ClickHouseDataSourceBuilder dsBuilder,
        Func<IServiceProvider, ClickHouseConnectionSettingsBuilder> builderFactory,
        ClickHouseConnectionOptions? pocoSnapshot)
    {
        // Capture provider delegates once; invoked per physical connection creation.
        var jwt = dsBuilder.JwtProviderFactory?.Invoke(sp);
        var cert = dsBuilder.CertificateProviderFactory?.Invoke(sp);
        var ssh = dsBuilder.SshKeyProviderFactory?.Invoke(sp);
        var pwd = dsBuilder.PasswordProviderFactory?.Invoke(sp);

        // Baseline once to capture any settings the builder produces deterministically
        // (host, port, TLS, compression, etc.). Used for ClickHouseDataSourceOptions.Settings.
        var baseline = builderFactory(sp).Build();

        // ConnectionFactory: rebuild from scratch on every creation, layering provider
        // outputs on top. This is the rotating-credential path.
        Func<CancellationToken, ValueTask<ClickHouseConnectionSettings>>? connectionFactory = null;
        if (jwt is not null || cert is not null || ssh is not null || pwd is not null)
        {
            connectionFactory = async ct =>
            {
                var b = builderFactory(sp);
                if (pwd is not null) b.WithPassword(await pwd(ct).ConfigureAwait(false));
                if (jwt is not null) b.WithJwt(await jwt(ct).ConfigureAwait(false));
                if (ssh is not null)
                {
                    var mat = await ssh(ct).ConfigureAwait(false);
                    b.WithSshKey(mat.PrivateKey, mat.Passphrase);
                }
                if (cert is not null)
                {
                    var x = await cert(ct).ConfigureAwait(false);
                    b.WithTls().WithTlsClientCertificate(x).WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate);
                }
                return b.Build();
            };
        }

        var options = BuildPoolOptions(baseline, connectionFactory, pocoSnapshot?.Pool, dsBuilder.PoolConfigurator);
        return new ClickHouseDataSource(options);
    }

    private static ClickHouseDataSourceOptions BuildPoolOptions(
        ClickHouseConnectionSettings baseline,
        Func<CancellationToken, ValueTask<ClickHouseConnectionSettings>>? connectionFactory,
        ClickHouseConnectionOptions.PoolOptions? pocoPool,
        Action<ClickHouseDataSourceOptions>? configurator)
    {
        // Start from record defaults, layer POCO-bound pool options, then the fluent
        // WithPool() override. The fluent layer gets final say.
        var defaults = new ClickHouseDataSourceOptions { Settings = baseline, ConnectionFactory = connectionFactory };

        if (pocoPool is null && configurator is null)
            return defaults;

        var options = new ClickHouseDataSourceOptions
        {
            Settings = baseline,
            ConnectionFactory = connectionFactory,
            MaxPoolSize = pocoPool?.MaxPoolSize ?? defaults.MaxPoolSize,
            MinPoolSize = pocoPool?.MinPoolSize ?? defaults.MinPoolSize,
            ConnectionIdleTimeout = pocoPool?.ConnectionIdleTimeout ?? defaults.ConnectionIdleTimeout,
            ConnectionLifetime = pocoPool?.ConnectionLifetime ?? defaults.ConnectionLifetime,
            ConnectionWaitTimeout = pocoPool?.ConnectionWaitTimeout ?? defaults.ConnectionWaitTimeout,
            ValidateOnRent = pocoPool?.ValidateOnRent ?? defaults.ValidateOnRent,
            PrewarmOnStart = pocoPool?.PrewarmOnStart ?? defaults.PrewarmOnStart,
        };

        configurator?.Invoke(options);
        return options;
    }

    private static ClickHouseConnectionSettingsBuilder WithConnectionString(
        this ClickHouseConnectionSettingsBuilder builder, string connectionString)
    {
        // Parse via the settings path, then replay into this builder so provider
        // delegates can still layer credentials on top.
        var parsed = ClickHouseConnectionSettings.Parse(connectionString);
        return CloneViaBuilder(parsed);
    }

    private static ClickHouseConnectionSettingsBuilder CloneViaBuilder(ClickHouseConnectionSettings s)
    {
        var b = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(s.Host)
            .WithPort(s.Port)
            .WithDatabase(s.Database)
            .WithUsername(s.Username)
            .WithClientName(s.ClientName)
            .WithCompression(s.Compress)
            .WithCompressionMethod(s.CompressionMethod);

        if (s.Password is { Length: > 0 }) b.WithPassword(s.Password);
        if (s.UseTls) b.WithTls().WithTlsPort(s.TlsPort);
        if (s.AllowInsecureTls) b.WithAllowInsecureTls();
        if (s.TlsCaCertificatePath is { Length: > 0 }) b.WithTlsCaCertificate(s.TlsCaCertificatePath);
        if (s.TlsClientCertificate is not null) b.WithTlsClientCertificate(s.TlsClientCertificate);
        if (s.Roles is { Count: > 0 }) b.WithRoles(s.Roles);
        if (s.JwtToken is { Length: > 0 }) b.WithJwt(s.JwtToken);
        if (!string.IsNullOrEmpty(s.SshPrivateKeyPath))
            b.WithSshKeyPath(s.SshPrivateKeyPath, s.SshPrivateKeyPassphrase);
        else if (s.SshPrivateKey is not null)
            b.WithSshKey(s.SshPrivateKey, s.SshPrivateKeyPassphrase);

        return b;
    }
}
