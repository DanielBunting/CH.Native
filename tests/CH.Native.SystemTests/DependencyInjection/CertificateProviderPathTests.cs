using System.Security.Cryptography.X509Certificates;
using CH.Native.Connection;
using CH.Native.DependencyInjection;
using CH.Native.SystemTests.Fixtures;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace CH.Native.SystemTests.DependencyInjection;

/// <summary>
/// End-to-end coverage for the mTLS rotating-credential path:
/// <c>AddClickHouse(...).WithCertificateProvider&lt;T&gt;()</c>. The certificate is
/// supplied per physical connection by an <see cref="IClickHouseCertificateProvider"/>,
/// so the DataSource's eager metadata-only baseline build must tolerate the cert
/// being absent at registration time — the same deferral JWT and SSH already enjoy.
/// Companion to the builder-level <c>TlsClientCertificateValidationTests</c> and the
/// missing-cert guard in <c>MtlsMissingCertTests</c>.
/// </summary>
[Collection("ClickHouseAuth")]
[Trait(Categories.Name, Categories.DependencyInjection)]
public sealed class CertificateProviderPathTests
{
    private readonly ClickHouseAuthFixture _fx;

    public CertificateProviderPathTests(ClickHouseAuthFixture fx)
    {
        _fx = fx;
    }

    /// <summary>
    /// A certificate provider that returns the test fixture's client cert,
    /// resolved from DI — mirroring how a real app's provider would read from a
    /// cert store / Key Vault.
    /// </summary>
    private sealed class FixtureCertificateProvider : IClickHouseCertificateProvider
    {
        private readonly X509Certificate2 _cert;
        public FixtureCertificateProvider(X509Certificate2 cert) => _cert = cert;
        public ValueTask<X509Certificate2> GetCertificateAsync(CancellationToken cancellationToken)
            => new(_cert);
    }

    [Fact]
    public async Task CertificateProvider_ResolvesDataSource_AndHandshakesAsCertUser()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_fx.ClientCertificate);

        // The documented mTLS provider form (README / Hosting sample): auth method
        // is TlsClientCertificate, but the certificate itself comes from the provider
        // per connection — so the baseline build has no cert to validate.
        services.AddClickHouse("mtls", builder => builder
                .WithHost(_fx.Host)
                .WithTls()
                .WithTlsPort(_fx.TlsPort)
                .WithAllowInsecureTls()
                .WithUsername("cert_user")
                .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate))
            .WithCertificateProvider<FixtureCertificateProvider>();

        await using var provider = services.BuildServiceProvider();

        // Pre-fix this resolution threw InvalidOperationException
        // ("AuthMethod.TlsClientCertificate requires a client certificate ...")
        // from the eager baseline build, before any connection was attempted.
        var ds = provider.GetRequiredKeyedService<ClickHouseDataSource>("mtls");

        await using var conn = await ds.OpenConnectionAsync();
        Assert.Equal("cert_user", await conn.ExecuteScalarAsync<string>("SELECT currentUser()"));
    }
}
