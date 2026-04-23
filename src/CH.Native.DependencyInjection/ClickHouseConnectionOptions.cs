using CH.Native.Compression;
using CH.Native.Connection;

namespace CH.Native.DependencyInjection;

/// <summary>
/// POCO bound from <c>IConfiguration</c>. Lets a consumer declare a ClickHouse
/// connection entirely in <c>appsettings.json</c>. Not identical to
/// <see cref="ClickHouseConnectionSettings"/> — that type is immutable and
/// record-shaped; this one is mutable and binder-friendly.
///
/// Either <see cref="ConnectionString"/> is set (parsed as-is) or the flat
/// fields are used. If both are present the connection string wins and the
/// flat fields layer on top.
/// </summary>
public sealed class ClickHouseConnectionOptions
{
    /// <summary>Connection string — when set, parsed as-is via <see cref="ClickHouseConnectionSettings.Parse(string)"/>.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Host / server. Default <c>localhost</c>.</summary>
    public string? Host { get; set; }

    /// <summary>Port. Default 9000 / 9440 TLS.</summary>
    public int? Port { get; set; }

    /// <summary>Database. Default <c>default</c>.</summary>
    public string? Database { get; set; }

    /// <summary>Username.</summary>
    public string? Username { get; set; }

    /// <summary>Password — ignored when <see cref="AuthMethod"/> is not <c>Password</c>.</summary>
    public string? Password { get; set; }

    /// <summary>Whether to compress the wire protocol. Default true.</summary>
    public bool? Compress { get; set; }

    /// <summary>Compression method when <see cref="Compress"/> is true.</summary>
    public CompressionMethod? CompressionMethod { get; set; }

    /// <summary>Client name sent on handshake.</summary>
    public string? ClientName { get; set; }

    /// <summary>Default roles to activate on every query (<c>SET ROLE …</c>).</summary>
    public IList<string>? Roles { get; set; }

    // --- Auth ---
    /// <summary>Auth method. Default <c>Password</c>.</summary>
    public ClickHouseAuthMethod? AuthMethod { get; set; }

    /// <summary>Static JWT when <see cref="AuthMethod"/> is <c>Jwt</c>. Overridden by <see cref="IClickHouseJwtProvider"/>.</summary>
    public string? JwtToken { get; set; }

    /// <summary>Path to a PEM / OpenSSH private key file. Overridden by <see cref="IClickHouseSshKeyProvider"/>.</summary>
    public string? SshPrivateKeyPath { get; set; }

    /// <summary>Passphrase for an encrypted SSH key.</summary>
    public string? SshPrivateKeyPassphrase { get; set; }

    /// <summary>TLS client-cert configuration. Overridden by <see cref="IClickHouseCertificateProvider"/>.</summary>
    public TlsClientCertificateOptions? TlsClientCertificate { get; set; }

    // --- TLS ---
    /// <summary>Enable TLS.</summary>
    public bool? UseTls { get; set; }

    /// <summary>TLS port (default 9440).</summary>
    public int? TlsPort { get; set; }

    /// <summary>Skip certificate validation — testing only.</summary>
    public bool? AllowInsecureTls { get; set; }

    /// <summary>Optional CA certificate file for TLS validation.</summary>
    public string? TlsCaCertificatePath { get; set; }

    // --- Pool ---
    /// <summary>Pool tuning knobs. When null the defaults apply.</summary>
    public PoolOptions? Pool { get; set; }

    /// <summary>Bindable pool-config subsection.</summary>
    public sealed class PoolOptions
    {
        /// <inheritdoc cref="ClickHouseDataSourceOptions.MaxPoolSize"/>
        public int? MaxPoolSize { get; set; }
        /// <inheritdoc cref="ClickHouseDataSourceOptions.MinPoolSize"/>
        public int? MinPoolSize { get; set; }
        /// <inheritdoc cref="ClickHouseDataSourceOptions.ConnectionIdleTimeout"/>
        public TimeSpan? ConnectionIdleTimeout { get; set; }
        /// <inheritdoc cref="ClickHouseDataSourceOptions.ConnectionLifetime"/>
        public TimeSpan? ConnectionLifetime { get; set; }
        /// <inheritdoc cref="ClickHouseDataSourceOptions.ConnectionWaitTimeout"/>
        public TimeSpan? ConnectionWaitTimeout { get; set; }
        /// <inheritdoc cref="ClickHouseDataSourceOptions.ValidateOnRent"/>
        public bool? ValidateOnRent { get; set; }
        /// <inheritdoc cref="ClickHouseDataSourceOptions.PrewarmOnStart"/>
        public bool? PrewarmOnStart { get; set; }
    }
}

/// <summary>Bindable subsection describing an mTLS client certificate.</summary>
public sealed class TlsClientCertificateOptions
{
    /// <summary>Path to a .pfx / .p12 file. Mutually exclusive with <see cref="StoreName"/>.</summary>
    public string? PfxPath { get; set; }

    /// <summary>Optional password for <see cref="PfxPath"/>.</summary>
    public string? PfxPassword { get; set; }

    /// <summary>Cert store name (e.g. <c>My</c>). Mutually exclusive with <see cref="PfxPath"/>.</summary>
    public string? StoreName { get; set; }

    /// <summary>Cert store location — <c>CurrentUser</c> or <c>LocalMachine</c>.</summary>
    public string? StoreLocation { get; set; }

    /// <summary>Thumbprint to match when loading from a store.</summary>
    public string? Thumbprint { get; set; }
}
