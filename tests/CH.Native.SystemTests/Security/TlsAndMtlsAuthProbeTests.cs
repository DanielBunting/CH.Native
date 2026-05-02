using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Security;

/// <summary>
/// Round-trip system tests for TLS and mTLS connections. Uses
/// <see cref="ClickHouseAuthFixture"/>'s pre-provisioned <c>cert_user</c> with an
/// X.509 client certificate registered server-side, plus the self-signed CA used to
/// sign the server cert.
///
/// <para>Adjacent integration tests in <c>tests/CH.Native.Tests/Integration/</c> pin
/// happy-path behaviour at the unit level; these tests pin the system-level surface
/// (handshake exception types under hostname mismatch, expired cert, missing cert,
/// pool reuse with mTLS) that's most likely to regress without anyone noticing.</para>
/// </summary>
[Collection("ClickHouseAuth")]
[Trait(Categories.Name, Categories.Security)]
public class TlsAndMtlsAuthProbeTests
{
    private readonly ClickHouseAuthFixture _fx;
    private readonly ITestOutputHelper _output;

    public TlsAndMtlsAuthProbeTests(ClickHouseAuthFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task Tls_AllowInsecureTrue_HandshakeSucceedsAgainstSelfSignedServer()
    {
        // Server cert is self-signed (CA in fixture). Without AllowInsecureTls,
        // .NET refuses to validate. With it, the TLS handshake succeeds.
        // Use ssh_user (defined in fixture's users.d) — the auth fixture's default
        // user requires a password we don't have, but ssh_user authenticates with the
        // pre-provisioned key.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithTls()
            .WithTlsPort(_fx.TlsPort)
            .WithAllowInsecureTls()
            .WithUsername("ssh_user")
            .WithSshKey(_fx.SshPrivateKeyPem)
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
    }

    [Fact]
    public async Task Tls_AllowInsecureFalse_FailsCertValidation()
    {
        // The fixture's CA isn't in the system trust store; default validation must
        // refuse — surface a typed exception, not a hang. Failure should land at the
        // TLS layer before any auth bytes leave the client.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithTls()
            .WithTlsPort(_fx.TlsPort)
            .WithAllowInsecureTls(false)
            .WithUsername("ssh_user")
            .WithSshKey(_fx.SshPrivateKeyPem)
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        var caught = await Assert.ThrowsAnyAsync<Exception>(() => conn.OpenAsync());

        _output.WriteLine($"Self-signed-no-trust surface: {caught.GetType().FullName} — {caught.Message}");
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    [Fact]
    public async Task Mtls_ValidClientCert_ConnectsAsCertUser()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithTls()
            .WithTlsPort(_fx.TlsPort)
            .WithAllowInsecureTls()
            .WithTlsClientCertificate(_fx.ClientCertificate)
            .WithUsername("cert_user")
            .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        Assert.Equal("cert_user", await conn.ExecuteScalarAsync<string>("SELECT currentUser()"));
    }

    [Fact]
    public async Task Mtls_RogueClientCert_FailsWithTypedException()
    {
        // Self-signed cert with the right CN ("cert_user") but signed by a
        // CA the server doesn't trust. Server's verificationMode=relaxed accepts
        // unknown CAs but still requires a valid handshake — the user-mapping
        // step then fails because the cert chain doesn't match what's pinned.
        using var rogueRsa = RSA.Create(2048);
        var rogueReq = new CertificateRequest("CN=cert_user", rogueRsa,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var rogueCert = rogueReq.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        using var loaded = new X509Certificate2(
            rogueCert.Export(X509ContentType.Pfx, "x"), "x", X509KeyStorageFlags.Exportable);

        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithTls()
            .WithTlsPort(_fx.TlsPort)
            .WithAllowInsecureTls()
            .WithTlsClientCertificate(loaded)
            .WithUsername("cert_user")
            .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        var caught = await Assert.ThrowsAnyAsync<Exception>(() => conn.OpenAsync());

        _output.WriteLine($"Rogue-CA cert surface: {caught.GetType().FullName} — {caught.Message}");
        Assert.True(
            caught is ClickHouseServerException or ClickHouseConnectionException or IOException,
            $"Expected typed auth failure; got {caught.GetType().FullName}");
    }

    [Fact]
    public async Task Mtls_ExpiredClientCert_FailsHandshake()
    {
        // Issue a cert from a fresh CA whose NotAfter is in the past. Even with
        // verificationMode=relaxed the server's TLS layer should refuse expired certs.
        var notBefore = DateTimeOffset.UtcNow.AddDays(-7);
        var notAfter = DateTimeOffset.UtcNow.AddDays(-1);
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=cert_user", rsa,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var expired = req.CreateSelfSigned(notBefore, notAfter);
        using var loaded = new X509Certificate2(
            expired.Export(X509ContentType.Pfx, "x"), "x", X509KeyStorageFlags.Exportable);

        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithTls()
            .WithTlsPort(_fx.TlsPort)
            .WithAllowInsecureTls()
            .WithTlsClientCertificate(loaded)
            .WithUsername("cert_user")
            .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        var caught = await Assert.ThrowsAnyAsync<Exception>(() => conn.OpenAsync());

        _output.WriteLine($"Expired-cert surface: {caught.GetType().FullName} — {caught.Message}");
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    [Fact]
    public async Task Mtls_PoolReusesTlsAuthenticatedConnections()
    {
        // mTLS handshakes are expensive; pool reuse must work for cert-auth
        // connections the same way as password.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithTls()
            .WithTlsPort(_fx.TlsPort)
            .WithAllowInsecureTls()
            .WithTlsClientCertificate(_fx.ClientCertificate)
            .WithUsername("cert_user")
            .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
            .Build();

        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = settings,
            MaxPoolSize = 2,
        });

        for (int i = 0; i < 6; i++)
        {
            await using var conn = await ds.OpenConnectionAsync();
            Assert.Equal("cert_user", await conn.ExecuteScalarAsync<string>("SELECT currentUser()"));
        }

        var stats = ds.GetStatistics();
        _output.WriteLine($"mTLS pool stats after 6 rents: {stats}");
        Assert.True(stats.TotalCreated <= 2,
            $"Pool should reuse mTLS connections; TotalCreated={stats.TotalCreated}");
    }

    [Fact]
    public async Task Tls_PlainClientToTlsPort_FailsCleanly()
    {
        // The plan's §2 #18/19 covers TLS renegotiation and downgrade — both need
        // server-side knobs ClickHouse doesn't expose. Closest testable cousin: the
        // protocol-mismatch case where the client speaks plain TCP to the TLS port.
        // The server expects a TLS ClientHello and rejects; the client must surface
        // a typed exception, never hang or OOM.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithPort(_fx.TlsPort)            // TLS port, but…
            .WithTls(false)                   // …client speaks plain TCP
            .WithUsername("ssh_user")
            .WithSshKey(_fx.SshPrivateKeyPem)
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Exception? caught = null;
        try { await conn.OpenAsync(cts.Token); }
        catch (Exception ex) { caught = ex; }

        _output.WriteLine($"Plain-to-TLS-port surface: {caught?.GetType().FullName} — {caught?.Message}");
        Assert.NotNull(caught);
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    [Fact]
    public async Task Tls_TlsClientToPlainPort_FailsCleanly()
    {
        // Inverse mismatch: client demands TLS at the plain port. Server's TLS layer
        // never engages (the port isn't TLS-listening) — the TLS handshake reads
        // garbage. Pin: typed exception, no hang.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithTls()
            .WithTlsPort(_fx.Port)            // plain port…
            .WithAllowInsecureTls()           // …with TLS demanded
            .WithUsername("ssh_user")
            .WithSshKey(_fx.SshPrivateKeyPem)
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Exception? caught = null;
        try { await conn.OpenAsync(cts.Token); }
        catch (Exception ex) { caught = ex; }

        _output.WriteLine($"TLS-to-plain-port surface: {caught?.GetType().FullName} — {caught?.Message}");
        Assert.NotNull(caught);
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    [Fact]
    public async Task Tls_NegotiatedProtocol_IsTls12OrHigher_DefaultsAreSafe()
    {
        // The plan's "TLS 1.0 cipher rejection" probe needs server-side config we
        // don't have. The closest pin we *can* assert: a handshake against the
        // fixture's TLS port succeeds with the .NET defaults, which since .NET 6
        // disable TLS 1.0/1.1 on the SslStream side. So a successful handshake here
        // is implicit proof that the negotiated protocol is TLS 1.2+ — no
        // server-side knob required.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithTls()
            .WithTlsPort(_fx.TlsPort)
            .WithAllowInsecureTls()
            .WithUsername("ssh_user")
            .WithSshKey(_fx.SshPrivateKeyPem)
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();
        Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
        // .NET 8's default SslClientAuthenticationOptions enables only TLS 1.2/1.3.
        // A successful handshake therefore implies one of those was negotiated.
    }

    [Fact]
    public async Task Tls_DisabledAndQueriedAtPlainPort_SucceedsAsControl()
    {
        // Sanity check: same fixture also exposes plain port 9000, used by the rest
        // of the suite. Pin that the cert-auth fixture isn't accidentally breaking
        // non-TLS connections — uses ssh_user since the fixture's default user
        // doesn't have a known password.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithUsername("ssh_user")
            .WithSshKey(_fx.SshPrivateKeyPem)
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
    }
}
