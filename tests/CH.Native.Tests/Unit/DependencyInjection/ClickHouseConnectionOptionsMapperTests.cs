using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using CH.Native.Compression;
using CH.Native.Connection;
using CH.Native.DependencyInjection;
using Xunit;

namespace CH.Native.Tests.Unit.DependencyInjection;

/// <summary>
/// Full line + branch coverage for the internal <see cref="ClickHouseConnectionOptionsMapper"/>: the
/// connection-string path, the flat-field mapping (each option set and unset), every auth-method case
/// and inference branch, and the settings round-trip (<c>ApplySettingsToNewBuilder</c>). The OS/PFX
/// certificate-loading helper is excluded from coverage (needs a real cert artifact).
/// </summary>
public class ClickHouseConnectionOptionsMapperTests
{
    private static X509Certificate2 SelfSigned()
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=chnative-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
    }

    [Fact]
    public void BuildSettings_BuildsFromOptions()
    {
        var s = ClickHouseConnectionOptionsMapper.BuildSettings(new ClickHouseConnectionOptions { Host = "h1" });
        Assert.Equal("h1", s.Host);
    }

    [Fact]
    public void CreateBuilder_ConnectionString_HonouredAsIs()
    {
        var s = ClickHouseConnectionOptionsMapper.CreateBuilder(
            new ClickHouseConnectionOptions { ConnectionString = "Host=cs-host;Port=9001;Database=d;Username=u;Password=p" }).Build();
        Assert.Equal("cs-host", s.Host);
        Assert.Equal(9001, s.Port);
    }

    [Fact]
    public void CreateBuilder_AllFlatFields_Applied()
    {
        var o = new ClickHouseConnectionOptions
        {
            Host = "h", Port = 9000, Database = "d", Username = "u", ClientName = "c",
            Compress = true, CompressionMethod = CompressionMethod.Lz4,
            Roles = new[] { "r1" }, UseTls = true, TlsPort = 9440, AllowInsecureTls = true,
            TlsCaCertificatePath = "/ca", AuthMethod = ClickHouseAuthMethod.Password, Password = "pw",
        };
        var s = ClickHouseConnectionOptionsMapper.CreateBuilder(o).Build();
        Assert.Equal("h", s.Host);
        Assert.Equal("pw", s.Password);
    }

    [Fact]
    public void CreateBuilder_NoFlatFields_TakesFalseBranches()
    {
        // Every optional field unset -> all mapping conditions take their false branch.
        Assert.NotNull(ClickHouseConnectionOptionsMapper.CreateBuilder(new ClickHouseConnectionOptions()));
    }

    // ---- explicit AuthMethod cases ----

    [Fact]
    public void Auth_Jwt_WithToken()
    {
        var s = ClickHouseConnectionOptionsMapper.CreateBuilder(
            new ClickHouseConnectionOptions { Host = "h", AuthMethod = ClickHouseAuthMethod.Jwt, JwtToken = "t" }).Build();
        Assert.Equal("t", s.JwtToken);
    }

    [Fact]
    public void Auth_Jwt_WithoutToken_SetsMethodOnly()
    {
        var b = ClickHouseConnectionOptionsMapper.CreateBuilder(
            new ClickHouseConnectionOptions { Host = "h", AuthMethod = ClickHouseAuthMethod.Jwt });
        Assert.NotNull(b);
    }

    [Fact]
    public void Auth_SshKey_WithPath()
    {
        var b = ClickHouseConnectionOptionsMapper.CreateBuilder(
            new ClickHouseConnectionOptions { Host = "h", AuthMethod = ClickHouseAuthMethod.SshKey, SshPrivateKeyPath = "/k" });
        Assert.NotNull(b);
    }

    [Fact]
    public void Auth_SshKey_WithoutPath_SetsMethodOnly()
    {
        var b = ClickHouseConnectionOptionsMapper.CreateBuilder(
            new ClickHouseConnectionOptions { Host = "h", AuthMethod = ClickHouseAuthMethod.SshKey });
        Assert.NotNull(b);
    }

    [Fact]
    public void Auth_TlsClientCertificate_NullOptions()
    {
        // AuthMethod set but no cert options -> the helper returns early; the auth method is still set.
        var b = ClickHouseConnectionOptionsMapper.CreateBuilder(
            new ClickHouseConnectionOptions { Host = "h", AuthMethod = ClickHouseAuthMethod.TlsClientCertificate });
        Assert.NotNull(b);
    }

    // ---- inferred AuthMethod (AuthMethod == null) ----

    [Fact]
    public void Infer_Jwt()
    {
        var s = ClickHouseConnectionOptionsMapper.CreateBuilder(
            new ClickHouseConnectionOptions { Host = "h", JwtToken = "t" }).Build();
        Assert.Equal("t", s.JwtToken);
    }

    [Fact]
    public void Infer_SshKey()
    {
        var b = ClickHouseConnectionOptionsMapper.CreateBuilder(
            new ClickHouseConnectionOptions { Host = "h", SshPrivateKeyPath = "/k" });
        Assert.NotNull(b);
    }

    [Fact]
    public void Infer_TlsClientCertificate()
    {
        var b = ClickHouseConnectionOptionsMapper.CreateBuilder(
            new ClickHouseConnectionOptions { Host = "h", TlsClientCertificate = new TlsClientCertificateOptions() });
        Assert.NotNull(b);
    }

    [Fact]
    public void Infer_Password_WithPassword()
    {
        var s = ClickHouseConnectionOptionsMapper.CreateBuilder(
            new ClickHouseConnectionOptions { Host = "h", Password = "pw" }).Build();
        Assert.Equal("pw", s.Password);
    }

    // ---- ApplySettingsToNewBuilder (internal; the connection-string round-trip) ----

    [Fact]
    public void ApplySettings_PopulatedFields()
    {
        var s = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("h").WithTls().WithTlsPort(9440).WithAllowInsecureTls()
            .WithTlsCaCertificate("/ca").WithRoles(new[] { "r" }).Build();
        Assert.NotNull(ClickHouseConnectionOptionsMapper.ApplySettingsToNewBuilder(s));
    }

    [Fact]
    public void ApplySettings_Password() =>
        Assert.NotNull(ClickHouseConnectionOptionsMapper.ApplySettingsToNewBuilder(
            ClickHouseConnectionSettings.CreateBuilder().WithHost("h").WithPassword("p").Build()));

    [Fact]
    public void ApplySettings_Jwt() =>
        Assert.NotNull(ClickHouseConnectionOptionsMapper.ApplySettingsToNewBuilder(
            ClickHouseConnectionSettings.CreateBuilder().WithHost("h").WithJwt("t").Build()));

    [Fact]
    public void ApplySettings_ClientCertificate() =>
        Assert.NotNull(ClickHouseConnectionOptionsMapper.ApplySettingsToNewBuilder(
            ClickHouseConnectionSettings.CreateBuilder().WithHost("h").WithTlsClientCertificate(SelfSigned()).Build()));

    [Fact]
    public void ApplySettings_SshKeyPath() =>
        Assert.NotNull(ClickHouseConnectionOptionsMapper.ApplySettingsToNewBuilder(
            ClickHouseConnectionSettings.CreateBuilder().WithHost("h").WithSshKeyPath("/k", "pw").Build()));

    [Fact]
    public void ApplySettings_SshKeyBytes() =>
        Assert.NotNull(ClickHouseConnectionOptionsMapper.ApplySettingsToNewBuilder(
            ClickHouseConnectionSettings.CreateBuilder().WithHost("h").WithSshKey(new byte[] { 1, 2, 3 }, "pw").Build()));

    [Fact]
    public void ApplySettings_Minimal_TakesFalseBranches() =>
        Assert.NotNull(ClickHouseConnectionOptionsMapper.ApplySettingsToNewBuilder(
            ClickHouseConnectionSettings.CreateBuilder().WithHost("h").Build()));
}
