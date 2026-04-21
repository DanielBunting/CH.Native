using System.Security.Cryptography.X509Certificates;
using CH.Native.Connection;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

public class ClickHouseAuthMethodTests
{
    [Fact]
    public void Default_AuthMethod_IsPassword()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .Build();

        Assert.Equal(ClickHouseAuthMethod.Password, settings.AuthMethod);
        Assert.Null(settings.JwtToken);
        Assert.Null(settings.SshPrivateKey);
    }

    [Fact]
    public void WithPassword_KeepsPasswordAuthMethod()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithPassword("secret")
            .Build();

        Assert.Equal(ClickHouseAuthMethod.Password, settings.AuthMethod);
        Assert.Equal("secret", settings.Password);
    }

    [Fact]
    public void WithJwt_SetsJwtAuthMethod()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithJwt("eyJhbGciOi.test.token")
            .Build();

        Assert.Equal(ClickHouseAuthMethod.Jwt, settings.AuthMethod);
        Assert.Equal("eyJhbGciOi.test.token", settings.JwtToken);
    }

    [Fact]
    public void WithJwt_EmptyToken_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.CreateBuilder().WithJwt(""));
    }

    [Fact]
    public void WithJwt_NullToken_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.CreateBuilder().WithJwt(null!));
    }

    [Fact]
    public void WithSshKey_Bytes_SetsSshAuthMethod()
    {
        var key = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };

        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithSshKey(key, passphrase: "secret")
            .Build();

        Assert.Equal(ClickHouseAuthMethod.SshKey, settings.AuthMethod);
        Assert.Equal(key, settings.SshPrivateKey);
        Assert.Null(settings.SshPrivateKeyPath);
        Assert.Equal("secret", settings.SshPrivateKeyPassphrase);
    }

    [Fact]
    public void WithSshKeyPath_SetsSshAuthMethod()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithSshKeyPath("/keys/id_ed25519")
            .Build();

        Assert.Equal(ClickHouseAuthMethod.SshKey, settings.AuthMethod);
        Assert.Equal("/keys/id_ed25519", settings.SshPrivateKeyPath);
        Assert.Null(settings.SshPrivateKey);
    }

    [Fact]
    public void WithSshKey_EmptyBytes_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.CreateBuilder().WithSshKey(Array.Empty<byte>()));
    }

    [Fact]
    public void WithSshKeyPath_Empty_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.CreateBuilder().WithSshKeyPath(""));
    }

    [Fact]
    public void Build_PasswordAndJwt_Throws()
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithPassword("secret")
            .WithJwt("token");

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void Build_JwtAndSsh_Throws()
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithJwt("token")
            .WithSshKeyPath("/keys/x");

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void Build_PasswordAndSsh_Throws()
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithPassword("secret")
            .WithSshKeyPath("/keys/x");

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void Build_TlsClientCertificate_WithoutTls_Throws()
    {
        using var cert = SelfSignedCert();
        var builder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithTlsClientCertificate(cert)
            .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate);

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("TLS", ex.Message);
    }

    [Fact]
    public void Build_TlsClientCertificate_WithoutCert_Throws()
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithTls()
            .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate);

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("client certificate", ex.Message);
    }

    [Fact]
    public void Build_TlsClientCertificate_Succeeds_WithTlsAndCert()
    {
        using var cert = SelfSignedCert();
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithTls()
            .WithTlsClientCertificate(cert)
            .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
            .Build();

        Assert.Equal(ClickHouseAuthMethod.TlsClientCertificate, settings.AuthMethod);
        Assert.Same(cert, settings.TlsClientCertificate);
    }

    [Fact]
    public void Parse_JwtKey_SetsJwtAuthMethod()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Jwt=tok123");

        Assert.Equal(ClickHouseAuthMethod.Jwt, settings.AuthMethod);
        Assert.Equal("tok123", settings.JwtToken);
    }

    [Fact]
    public void Parse_TokenAlias_SetsJwtAuthMethod()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Token=tok123");

        Assert.Equal(ClickHouseAuthMethod.Jwt, settings.AuthMethod);
        Assert.Equal("tok123", settings.JwtToken);
    }

    [Fact]
    public void Parse_SshKeyPath_SetsSshAuthMethod()
    {
        var settings = ClickHouseConnectionSettings.Parse(
            "Host=localhost;SshKeyPath=/keys/id;SshKeyPassphrase=pw");

        Assert.Equal(ClickHouseAuthMethod.SshKey, settings.AuthMethod);
        Assert.Equal("/keys/id", settings.SshPrivateKeyPath);
        Assert.Equal("pw", settings.SshPrivateKeyPassphrase);
    }

    [Fact]
    public void Parse_SshPassphraseWithoutPath_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.Parse("Host=localhost;SshKeyPassphrase=pw"));
    }

    [Fact]
    public void Parse_JwtAndPassword_Throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
            ClickHouseConnectionSettings.Parse("Host=localhost;Password=x;Jwt=y"));
    }

    [Fact]
    public void Parse_ClientCertificatePath_LoadsCertificate()
    {
        var pfxPath = Path.GetTempFileName();
        try
        {
            using var cert = SelfSignedCert();
            File.WriteAllBytes(pfxPath, cert.Export(X509ContentType.Pfx, "testpass"));

            var settings = ClickHouseConnectionSettings.Parse(
                $"Host=localhost;Tls=true;ClientCertificatePath={pfxPath};ClientCertificatePassword=testpass");

            Assert.NotNull(settings.TlsClientCertificate);
            Assert.Equal(cert.Thumbprint, settings.TlsClientCertificate!.Thumbprint);
        }
        finally
        {
            if (File.Exists(pfxPath)) File.Delete(pfxPath);
        }
    }

    [Fact]
    public void Parse_ClientCertificatePasswordWithoutPath_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.Parse("Host=localhost;ClientCertificatePassword=pw"));
    }

    [Fact]
    public void WithTlsClientCertificate_FromPath_Loads()
    {
        var pfxPath = Path.GetTempFileName();
        try
        {
            using var cert = SelfSignedCert();
            File.WriteAllBytes(pfxPath, cert.Export(X509ContentType.Pfx, "testpass"));

            var settings = ClickHouseConnectionSettings.CreateBuilder()
                .WithHost("localhost")
                .WithTls()
                .WithTlsClientCertificate(pfxPath, "testpass")
                .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
                .Build();

            Assert.NotNull(settings.TlsClientCertificate);
            Assert.Equal(cert.Thumbprint, settings.TlsClientCertificate!.Thumbprint);
        }
        finally
        {
            if (File.Exists(pfxPath)) File.Delete(pfxPath);
        }
    }

    private static X509Certificate2 SelfSignedCert()
    {
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var req = new CertificateRequest("CN=test", rsa,
            System.Security.Cryptography.HashAlgorithmName.SHA256,
            System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        return req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
    }
}
