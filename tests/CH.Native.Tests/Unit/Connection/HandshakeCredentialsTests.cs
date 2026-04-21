using System.Buffers;
using CH.Native.Connection;
using CH.Native.Protocol;
using CH.Native.Protocol.Messages;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

public class HandshakeCredentialsTests
{
    [Fact]
    public void Password_UsesConfiguredUsernameAndPassword()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithUsername("alice")
            .WithPassword("s3cr3t")
            .Build();

        var (user, pass) = ClickHouseConnection.BuildHandshakeCredentials(settings);

        Assert.Equal("alice", user);
        Assert.Equal("s3cr3t", pass);
    }

    [Fact]
    public void Jwt_EmitsMarkerUsernameAndTokenPassword()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithUsername("ignored-when-jwt")
            .WithJwt("eyJ.dummy.token")
            .Build();

        var (user, pass) = ClickHouseConnection.BuildHandshakeCredentials(settings);

        Assert.Equal(" JWT AUTHENTICATION ", user);
        Assert.Equal("eyJ.dummy.token", pass);
    }

    [Fact]
    public void SshKey_EmitsMarkerPlusUsernameAndEmptyPassword()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithUsername("bob")
            .WithSshKeyPath("/keys/id_ed25519")
            .Build();

        var (user, pass) = ClickHouseConnection.BuildHandshakeCredentials(settings);

        Assert.Equal(" SSH KEY AUTHENTICATION bob", user);
        Assert.Equal("", pass);
    }

    [Fact]
    public void TlsClientCertificate_UsesUsernameAndEmptyPassword()
    {
        using var cert = CertHelper.SelfSigned();
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithUsername("cert_user")
            .WithTls()
            .WithTlsClientCertificate(cert)
            .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
            .Build();

        var (user, pass) = ClickHouseConnection.BuildHandshakeCredentials(settings);

        Assert.Equal("cert_user", user);
        Assert.Equal("", pass);
    }

    [Fact]
    public void Jwt_ClientHelloWireFormat_EmitsMarkerThenToken()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithUsername("ignored")
            .WithDatabase("mydb")
            .WithJwt("T0K3N")
            .Build();

        var (user, pass) = ClickHouseConnection.BuildHandshakeCredentials(settings);
        var hello = ClientHello.Create("CH.Native", settings.Database, user, pass);

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        hello.Write(ref writer);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        Assert.Equal(0UL, reader.ReadVarInt());           // message type
        Assert.Equal("CH.Native", reader.ReadString());   // client name
        reader.ReadVarInt(); reader.ReadVarInt(); reader.ReadVarInt();
        Assert.Equal("mydb", reader.ReadString());
        Assert.Equal(" JWT AUTHENTICATION ", reader.ReadString());
        Assert.Equal("T0K3N", reader.ReadString());
        Assert.Equal(0, reader.Remaining);
    }
}

internal static class CertHelper
{
    public static System.Security.Cryptography.X509Certificates.X509Certificate2 SelfSigned()
    {
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var req = new System.Security.Cryptography.X509Certificates.CertificateRequest(
            "CN=test", rsa,
            System.Security.Cryptography.HashAlgorithmName.SHA256,
            System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        return req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
    }
}
