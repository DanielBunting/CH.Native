using System.Security.Cryptography.X509Certificates;
using CH.Native.Connection;

namespace CH.Native.DependencyInjection;

internal static class ClickHouseConnectionOptionsMapper
{
    /// <summary>
    /// Builds final settings from the POCO. For static auth this is all that's
    /// needed; for provider-driven auth use <see cref="CreateBuilder"/> and layer
    /// provider outputs on top before calling <c>Build</c>.
    /// </summary>
    internal static ClickHouseConnectionSettings BuildSettings(ClickHouseConnectionOptions options)
        => CreateBuilder(options).Build();

    /// <summary>
    /// Produces a <see cref="ClickHouseConnectionSettingsBuilder"/> populated
    /// from <paramref name="options"/>. Callers can chain additional <c>With*</c>
    /// calls (e.g. provider-sourced credentials) before building.
    /// </summary>
    internal static ClickHouseConnectionSettingsBuilder CreateBuilder(ClickHouseConnectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        // If a raw connection string is supplied, honour it as-is. Flat overrides
        // applied from the POCO are intentionally ignored — callers pick one shape.
        if (!string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            var parsed = ClickHouseConnectionSettings.Parse(options.ConnectionString);
            return ApplySettingsToNewBuilder(parsed);
        }

        var builder = ClickHouseConnectionSettings.CreateBuilder();

        if (options.Host is { Length: > 0 }) builder.WithHost(options.Host);
        if (options.Port is { } port) builder.WithPort(port);
        if (options.Database is { Length: > 0 }) builder.WithDatabase(options.Database);
        if (options.Username is { Length: > 0 }) builder.WithUsername(options.Username);
        if (options.ClientName is { Length: > 0 }) builder.WithClientName(options.ClientName);

        if (options.Compress is { } compress) builder.WithCompression(compress);
        if (options.CompressionMethod is { } method) builder.WithCompressionMethod(method);

        if (options.Roles is { Count: > 0 }) builder.WithRoles(options.Roles);

        if (options.UseTls is true) builder.WithTls();
        if (options.TlsPort is { } tlsPort) builder.WithTlsPort(tlsPort);
        if (options.AllowInsecureTls is true) builder.WithAllowInsecureTls();
        if (options.TlsCaCertificatePath is { Length: > 0 }) builder.WithTlsCaCertificate(options.TlsCaCertificatePath);

        ApplyAuth(builder, options);

        return builder;
    }

    private static ClickHouseConnectionSettingsBuilder ApplySettingsToNewBuilder(ClickHouseConnectionSettings s)
    {
        // Best-effort round-trip: copies the fields that the builder exposes. Used
        // only on the ConnectionString path so that provider-driven auth can still
        // layer on top. Fields the builder cannot re-ingest (custom resilience
        // instances, telemetry) are lost on this round-trip — document as such.
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
        if (s.SshPrivateKeyPath is { Length: > 0 }) b.WithSshKeyPath(s.SshPrivateKeyPath, s.SshPrivateKeyPassphrase);
        else if (s.SshPrivateKey is not null) b.WithSshKey(s.SshPrivateKey, s.SshPrivateKeyPassphrase);
        return b;
    }

    private static void ApplyAuth(ClickHouseConnectionSettingsBuilder builder, ClickHouseConnectionOptions options)
    {
        var method = options.AuthMethod ?? InferAuthMethod(options);
        switch (method)
        {
            case ClickHouseAuthMethod.Password:
                if (options.Password is { Length: > 0 })
                    builder.WithPassword(options.Password);
                break;

            case ClickHouseAuthMethod.Jwt:
                if (options.JwtToken is { Length: > 0 })
                    builder.WithJwt(options.JwtToken);
                else
                    builder.WithAuthMethod(ClickHouseAuthMethod.Jwt);
                break;

            case ClickHouseAuthMethod.SshKey:
                if (options.SshPrivateKeyPath is { Length: > 0 })
                    builder.WithSshKeyPath(options.SshPrivateKeyPath, options.SshPrivateKeyPassphrase);
                else
                    builder.WithAuthMethod(ClickHouseAuthMethod.SshKey);
                break;

            case ClickHouseAuthMethod.TlsClientCertificate:
                ApplyTlsClientCertificate(builder, options.TlsClientCertificate);
                builder.WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate);
                break;
        }
    }

    private static ClickHouseAuthMethod InferAuthMethod(ClickHouseConnectionOptions options)
    {
        if (!string.IsNullOrEmpty(options.JwtToken)) return ClickHouseAuthMethod.Jwt;
        if (!string.IsNullOrEmpty(options.SshPrivateKeyPath)) return ClickHouseAuthMethod.SshKey;
        if (options.TlsClientCertificate is not null) return ClickHouseAuthMethod.TlsClientCertificate;
        return ClickHouseAuthMethod.Password;
    }

    private static void ApplyTlsClientCertificate(ClickHouseConnectionSettingsBuilder builder, TlsClientCertificateOptions? tls)
    {
        if (tls is null) return;

        if (!string.IsNullOrEmpty(tls.PfxPath))
        {
            builder.WithTlsClientCertificate(tls.PfxPath, tls.PfxPassword);
            return;
        }

        if (!string.IsNullOrEmpty(tls.StoreName) && !string.IsNullOrEmpty(tls.Thumbprint))
        {
            var location = Enum.TryParse<StoreLocation>(tls.StoreLocation, ignoreCase: true, out var loc)
                ? loc
                : StoreLocation.CurrentUser;
            using var store = new X509Store(tls.StoreName, location);
            store.Open(OpenFlags.ReadOnly);
            var matches = store.Certificates.Find(X509FindType.FindByThumbprint, tls.Thumbprint, validOnly: false);
            if (matches.Count == 0)
                throw new InvalidOperationException(
                    $"TLS client certificate with thumbprint '{tls.Thumbprint}' not found in " +
                    $"{location}\\{tls.StoreName}.");
            builder.WithTlsClientCertificate(matches[0]);
        }
    }

}
