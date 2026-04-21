using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouseAuth")]
[Trait("Category", "Integration")]
public class TlsClientCertAuthTests
{
    private readonly ClickHouseAuthFixture _fixture;

    public TlsClientCertAuthTests(ClickHouseAuthFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ClientCertificate_Valid_ConnectsAsCertUser()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithTls()
            .WithTlsPort(_fixture.TlsPort)
            .WithAllowInsecureTls()
            .WithTlsClientCertificate(_fixture.ClientCertificate)
            .WithUsername("cert_user")
            .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        var principal = await connection.ExecuteScalarAsync<string>("SELECT currentUser()");
        Assert.Equal("cert_user", principal);
    }

    [Fact]
    public async Task ClientCertificate_WrongIssuer_Fails()
    {
        // Fresh self-signed CA that the server doesn't trust.
        using var rogueRsa = RSA.Create(2048);
        var rogueReq = new CertificateRequest("CN=cert_user", rogueRsa,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var rogueCert = rogueReq.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));

        using var loaded = new X509Certificate2(
            rogueCert.Export(X509ContentType.Pfx, "x"), "x", X509KeyStorageFlags.Exportable);

        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithTls()
            .WithTlsPort(_fixture.TlsPort)
            .WithAllowInsecureTls()
            .WithTlsClientCertificate(loaded)
            .WithUsername("cert_user")
            .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
            .Build();

        await using var connection = new ClickHouseConnection(settings);

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => connection.OpenAsync());
        Assert.True(ex is ClickHouseServerException or ClickHouseConnectionException or System.IO.IOException,
            $"expected auth failure, got {ex.GetType().Name}: {ex.Message}");
    }
}
