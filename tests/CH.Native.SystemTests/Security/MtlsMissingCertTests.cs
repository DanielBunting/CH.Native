using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Security;

/// <summary>
/// Companion to <see cref="TlsAndMtlsAuthProbeTests"/>: pins the failure mode
/// when the user requests cert-auth but forgets to attach a client cert.
/// Existing tests cover rogue/expired certs; this fills the "no cert at all"
/// gap so the user gets a clear typed exception, not a confusing IOException.
/// </summary>
[Collection("ClickHouseAuth")]
[Trait(Categories.Name, Categories.Security)]
public class MtlsMissingCertTests
{
    private readonly ClickHouseAuthFixture _fx;
    private readonly ITestOutputHelper _output;

    public MtlsMissingCertTests(ClickHouseAuthFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public void Builder_TlsClientCertificateAuthMethod_WithoutCert_FailsAtBuildTime()
    {
        // Builder-level guard: if the user explicitly chooses
        // AuthMethod.TlsClientCertificate but never calls
        // WithTlsClientCertificate, the build should fail with a clear message
        // rather than letting the misconfiguration reach the wire.
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ClickHouseConnectionSettings.CreateBuilder()
                .WithHost(_fx.Host)
                .WithTls()
                .WithTlsPort(_fx.TlsPort)
                .WithAllowInsecureTls()
                .WithUsername("cert_user")
                .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
                .Build());

        _output.WriteLine($"Builder rejection: {ex.Message}");
        Assert.Contains("client certificate", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task PasswordUser_ConnectsViaTls_WithoutClientCert()
    {
        // Sanity counterpart to the above: TLS without mTLS works fine for
        // password users — the cert is only required when the user opts into
        // cert-based auth.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithTls()
            .WithTlsPort(_fx.TlsPort)
            .WithAllowInsecureTls()
            .WithUsername("default")
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        Assert.Equal("default", await conn.ExecuteScalarAsync<string>("SELECT currentUser()"));
    }

    [Fact]
    public void Builder_TlsClientCertificateAuthMethod_WithoutTls_FailsAtBuildTime()
    {
        // mTLS implies TLS — the auth method requires TLS to be enabled.
        // Pin the error message so users get a clear next step.
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ClickHouseConnectionSettings.CreateBuilder()
                .WithHost(_fx.Host)
                .WithUsername("cert_user")
                .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
                .Build());

        _output.WriteLine($"Builder rejection: {ex.Message}");
        Assert.Contains("TLS", ex.Message);
    }
}
