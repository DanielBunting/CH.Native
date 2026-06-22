using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using CH.Native.Connection;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;
#pragma warning disable SYSLIB0057 // legacy X509Certificate2 ctors in test cert setup; X509CertificateLoader is net9+ only and these projects also target net8.0

/// <summary>
/// Pre-fix <see cref="ClickHouseConnectionSettingsBuilder.WithTlsClientCertificate(X509Certificate2)"/>
/// accepted any certificate, including public-only certs that can't sign the
/// TLS handshake. The caller would then debug a server-side TLS-layer error
/// when the local config was the actual cause. The fix surfaces the missing
/// private key at builder time.
/// </summary>
public class TlsClientCertificateValidationTests
{
    [Fact]
    public void WithTlsClientCertificate_PublicOnlyCert_Throws()
    {
        // Generate a minimal self-signed cert, then strip the private key by
        // exporting and re-loading the public-only DER bytes.
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest(
            "CN=test",
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        using var withKey = req.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-5),
            DateTimeOffset.UtcNow.AddYears(1));
        using var publicOnly = new X509Certificate2(withKey.Export(X509ContentType.Cert));

        Assert.False(publicOnly.HasPrivateKey, "test setup: public-only cert");

        var builder = ClickHouseConnectionSettings.CreateBuilder();
        var ex = Assert.Throws<ArgumentException>(
            () => builder.WithTlsClientCertificate(publicOnly));
        Assert.Contains("private key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void WithTlsClientCertificate_CertWithKey_IsAccepted()
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest(
            "CN=test",
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        using var withKey = req.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-5),
            DateTimeOffset.UtcNow.AddYears(1));

        Assert.True(withKey.HasPrivateKey, "test setup: cert has key");

        // Should not throw.
        var builder = ClickHouseConnectionSettings.CreateBuilder()
            .WithTls()
            .WithTlsClientCertificate(withKey);
        Assert.NotNull(builder);
    }

    /// <summary>
    /// The DI rotating-credential path supplies the client certificate per
    /// physical connection via an <c>IClickHouseCertificateProvider</c>, but the
    /// DataSource still builds a metadata-only "baseline" settings object up front
    /// — before any provider runs. <see cref="ClickHouseConnectionSettingsBuilder.DeferClientCertificateValidation"/>
    /// lets that baseline build succeed with the cert absent, mirroring how JWT and
    /// SSH auth already defer their credential-presence checks.
    /// </summary>
    [Fact]
    public void Build_CertAuthMethod_NoCert_WithDeferral_Succeeds()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithTls()
            .WithUsername("cert_user")
            .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
            .DeferClientCertificateValidation()
            .Build();

        Assert.Equal(ClickHouseAuthMethod.TlsClientCertificate, settings.AuthMethod);
        Assert.Null(settings.TlsClientCertificate); // provider fills it in at connection-open time
    }

    /// <summary>
    /// Deferral is opt-in. A direct builder that selects cert auth but never
    /// attaches a cert (and never opts into deferral) must still fail fast — this
    /// pins the contract covered by the system-level MtlsMissingCertTests.
    /// </summary>
    [Fact]
    public void Build_CertAuthMethod_NoCert_WithoutDeferral_StillThrows()
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithTls()
            .WithUsername("cert_user")
            .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate);

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("client certificate", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Deferral relaxes only the certificate-presence check, not the TLS
    /// requirement — mTLS without TLS is still a misconfiguration.
    /// </summary>
    [Fact]
    public void Build_CertAuthMethod_NoTls_WithDeferral_StillThrows()
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithUsername("cert_user")
            .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
            .DeferClientCertificateValidation();

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("TLS", ex.Message);
    }
}
