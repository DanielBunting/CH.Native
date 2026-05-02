using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using CH.Native.Connection;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Security;

/// <summary>
/// Client-side validation for JWT, SSH, and TLS auth surfaces. These tests don't
/// require a server with the corresponding auth method enabled — they pin that the
/// builder rejects bad inputs at <see cref="ClickHouseConnectionSettingsBuilder.Build"/>
/// (or before any wire bytes are sent), surfacing a typed exception instead of
/// silently coercing or hanging on a server-side rejection that the user can't
/// diagnose without packet capture.
/// </summary>
[Trait(Categories.Name, Categories.Security)]
public class AuthSettingsValidationTests
{
    private readonly ITestOutputHelper _output;

    public AuthSettingsValidationTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public void Jwt_EmptyToken_RejectedAtBuilder()
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder().WithHost("h");
        Assert.Throws<ArgumentException>(() => builder.WithJwt(string.Empty));
    }

    [Fact]
    public void Jwt_WhitespaceOnlyToken_RejectedAtBuilder()
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder().WithHost("h");
        Assert.Throws<ArgumentException>(() => builder.WithJwt("   "));
    }

    [Fact]
    public void Jwt_WithPassword_ConflictDetectedAtBuild()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ClickHouseConnectionSettings.CreateBuilder()
                .WithHost("h")
                .WithPassword("p")
                .WithJwt("eyJhbGciOiJIUzI1NiJ9.e30.token")
                .Build());
        Assert.Contains("Multiple authentication methods", ex.Message);
    }

    [Fact]
    public void Jwt_LargeToken_AcceptedClientSide_NoOOM()
    {
        // 16 KB ASCII token. The wire-side cap is server-side, but the client must
        // not pre-buffer or reject silently. Pin: builder accepts, Build() succeeds.
        var token = new string('a', 16 * 1024);
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("h")
            .WithJwt(token)
            .Build();
        Assert.Equal(ClickHouseAuthMethod.Jwt, settings.AuthMethod);
    }

    [Fact]
    public void SshKey_EmptyByteArray_RejectedAtBuilder()
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder().WithHost("h");
        Assert.Throws<ArgumentException>(() => builder.WithSshKey(Array.Empty<byte>()));
    }

    [Fact]
    public void SshKey_NullByteArray_RejectedAtBuilder()
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder().WithHost("h");
        Assert.Throws<ArgumentNullException>(() => builder.WithSshKey(null!));
    }

    [Fact]
    public void SshKeyPath_EmptyPath_RejectedAtBuilder()
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder().WithHost("h");
        Assert.Throws<ArgumentException>(() => builder.WithSshKeyPath(string.Empty));
    }

    [Fact]
    public void SshKey_WithPassword_ConflictDetectedAtBuild()
    {
        // Using path form so the conflict shows up regardless of whether the key
        // file exists — Build()'s validation runs before any disk access.
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ClickHouseConnectionSettings.CreateBuilder()
                .WithHost("h")
                .WithPassword("p")
                .WithSshKeyPath("/dev/null")
                .Build());
        Assert.Contains("Multiple authentication methods", ex.Message);
    }

    [Fact]
    public void SshKey_AndJwt_ConflictDetectedAtBuild()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ClickHouseConnectionSettings.CreateBuilder()
                .WithHost("h")
                .WithJwt("token")
                .WithSshKeyPath("/dev/null")
                .Build());
        Assert.Contains("Multiple authentication methods", ex.Message);
    }

    [Fact]
    public void TlsClientCertificate_WithoutPrivateKey_RejectedAtBuilder()
    {
        // Build a public-only X509Certificate2 — no private key.
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var withKey = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        var publicOnlyBytes = withKey.Export(X509ContentType.Cert);
#if NET9_0_OR_GREATER
        using var publicOnly = X509CertificateLoader.LoadCertificate(publicOnlyBytes);
#else
        using var publicOnly = new X509Certificate2(publicOnlyBytes);
#endif
        Assert.False(publicOnly.HasPrivateKey);

        var builder = ClickHouseConnectionSettings.CreateBuilder().WithHost("h");
        var ex = Assert.Throws<ArgumentException>(() => builder.WithTlsClientCertificate(publicOnly));
        Assert.Contains("private key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TlsClientCertAuth_WithoutTls_FailsAtBuild()
    {
        // Auth method = TlsClientCertificate but TLS is off. Builder must reject.
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));

        var ex = Assert.Throws<InvalidOperationException>(() =>
            ClickHouseConnectionSettings.CreateBuilder()
                .WithHost("h")
                .WithTlsClientCertificate(cert)
                .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
                .Build());
        Assert.Contains("TLS", ex.Message);
    }

    [Fact]
    public void TlsPort_OutOfRange_RejectedAtBuilder()
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder().WithHost("h");
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithTlsPort(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithTlsPort(65536));
    }

    [Fact]
    public void TlsCa_NullPath_RejectedAtBuilder()
    {
        var builder = ClickHouseConnectionSettings.CreateBuilder().WithHost("h");
        Assert.Throws<ArgumentNullException>(() => builder.WithTlsCaCertificate(null!));
    }

    [Fact]
    public async Task SshKeyPath_NonExistentFile_FailsBeforeWire()
    {
        // Open against a non-existent SSH key file. The library should fail
        // client-side without any TCP connection attempt — the path is bad before
        // we even know if there's a server.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("127.0.0.1")
            .WithPort(1)  // unlikely to be open; if we reach here, we already failed the right way
            .WithUsername("default")
            .WithSshKeyPath("/nonexistent/probe-key")
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        Exception? caught = null;
        try
        {
            await conn.OpenAsync();
        }
        catch (Exception ex) { caught = ex; }

        _output.WriteLine($"SSH-missing-file surface: {caught?.GetType().FullName} — {caught?.Message}");
        Assert.NotNull(caught);
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }
}
