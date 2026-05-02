using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using CH.Native.Connection;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

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
}
