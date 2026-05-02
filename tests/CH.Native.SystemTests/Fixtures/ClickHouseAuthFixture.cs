using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using Xunit;

namespace CH.Native.SystemTests.Fixtures;

/// <summary>
/// Testcontainers-backed ClickHouse server with three authentication users provisioned:
/// <c>ssh_user</c> (ssh-rsa public key), <c>cert_user</c> (X.509 client cert),
/// <c>jwt_user</c> (JWT — note: OSS server rejects JWT, usable only for wire-format round-trip tests),
/// plus the default <c>default</c>/no-password user.
/// </summary>
public sealed class ClickHouseAuthFixture : IAsyncLifetime
{
    private const int NativePort = 9000;
    private const int SecureNativePort = 9440;

    private IContainer? _container;
    private string? _tempDir;

    public string Host => _container?.Hostname ?? throw new InvalidOperationException("Container not started");
    public int Port => _container?.GetMappedPublicPort(NativePort) ?? throw new InvalidOperationException("Container not started");
    public int TlsPort => _container?.GetMappedPublicPort(SecureNativePort) ?? throw new InvalidOperationException("Container not started");

    /// <summary>PEM-encoded SSH private key bytes (PKCS#8 RSA).</summary>
    public byte[] SshPrivateKeyPem { get; private set; } = Array.Empty<byte>();

    /// <summary>Client X.509 certificate (loaded from the generated PFX).</summary>
    public X509Certificate2 ClientCertificate { get; private set; } = null!;

    /// <summary>CA certificate used to sign the server and client certs (for server-cert validation on the client).</summary>
    public X509Certificate2 CaCertificate { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"ch_auth_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);

        // --- 1. TLS certs: self-signed CA, server cert, client cert ---
        // Pin fixed timestamps so child certs' NotAfter never exceeds CA's
        // (which otherwise races by a few milliseconds and throws at .Create).
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        var caNotAfter = DateTimeOffset.UtcNow.AddYears(1);
        var childNotAfter = caNotAfter.AddMinutes(-1);

        using var caRsa = RSA.Create(2048);
        var caReq = new CertificateRequest("CN=ch-test-ca", caRsa,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        caReq.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign, true));
        using var ca = caReq.CreateSelfSigned(notBefore, caNotAfter);
        CaCertificate = new X509Certificate2(ca.Export(X509ContentType.Cert));

        using var serverRsa = RSA.Create(2048);
        var serverReq = new CertificateRequest("CN=localhost", serverRsa,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var sanBuilder = new SubjectAlternativeNameBuilder();
        sanBuilder.AddDnsName("localhost");
        sanBuilder.AddIpAddress(System.Net.IPAddress.Loopback);
        serverReq.CertificateExtensions.Add(sanBuilder.Build());
        serverReq.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        using var server = serverReq.Create(ca, notBefore, childNotAfter, Guid.NewGuid().ToByteArray());
        var serverCertPath = Path.Combine(_tempDir, "server.crt");
        var serverKeyPath = Path.Combine(_tempDir, "server.key");
        File.WriteAllText(serverCertPath, server.ExportCertificatePem());
        File.WriteAllText(serverKeyPath, serverRsa.ExportRSAPrivateKeyPem());

        using var clientRsa = RSA.Create(2048);
        var clientReq = new CertificateRequest("CN=cert_user", clientRsa,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        clientReq.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        clientReq.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            new OidCollection { new("1.3.6.1.5.5.7.3.2") /* clientAuth */ }, true));
        using var clientCert = clientReq.Create(ca, notBefore, childNotAfter, Guid.NewGuid().ToByteArray());
        using var clientCertWithKey = clientCert.CopyWithPrivateKey(clientRsa);
        ClientCertificate = new X509Certificate2(
            clientCertWithKey.Export(X509ContentType.Pfx, "test"), "test",
            X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet);

        // --- 2. SSH RSA keypair ---
        using var sshRsa = RSA.Create(2048);
        SshPrivateKeyPem = Encoding.UTF8.GetBytes(sshRsa.ExportPkcs8PrivateKeyPem());
        var sshPubOpenSsh = EncodeRsaOpenSshPublicKey(sshRsa);
        var sshPubBase64 = Convert.ToBase64String(sshPubOpenSsh);

        // --- 3. users.d overlay. No jwt_user: OSS ClickHouse rejects the <jwt>
        //     auth kind at config-parse time ("must be specified: password/...ssh_keys/...").
        //     JWT integration tests exercise only the client wire path, which hits
        //     the OSS's generic "Cloud-only" rejection — the user doesn't matter. ---
        var usersXml = $@"<clickhouse>
  <users>
    <ssh_user>
      <ssh_keys>
        <ssh_key>
          <type>ssh-rsa</type>
          <base64_key>{sshPubBase64}</base64_key>
        </ssh_key>
      </ssh_keys>
      <networks><ip>::/0</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
    </ssh_user>
    <cert_user>
      <ssl_certificates>
        <common_name>cert_user</common_name>
      </ssl_certificates>
      <networks><ip>::/0</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
    </cert_user>
  </users>
</clickhouse>";
        var usersPath = Path.Combine(_tempDir, "auth_test_users.xml");
        File.WriteAllText(usersPath, usersXml);

        // --- 4. config.d/tls.xml enabling port 9440 + mTLS ---
        var tlsConfig = @"<clickhouse>
  <tcp_port_secure>9440</tcp_port_secure>
  <openSSL>
    <server>
      <certificateFile>/etc/clickhouse-server/certs/server.crt</certificateFile>
      <privateKeyFile>/etc/clickhouse-server/certs/server.key</privateKeyFile>
      <caConfig>/etc/clickhouse-server/certs/ca.crt</caConfig>
      <verificationMode>relaxed</verificationMode>
      <loadDefaultCAFile>false</loadDefaultCAFile>
      <disableProtocols>sslv2,sslv3</disableProtocols>
    </server>
  </openSSL>
</clickhouse>";
        var tlsConfigPath = Path.Combine(_tempDir, "tls.xml");
        File.WriteAllText(tlsConfigPath, tlsConfig);

        var caCertPath = Path.Combine(_tempDir, "ca.crt");
        File.WriteAllText(caCertPath, ca.ExportCertificatePem());

        // --- 5. Start container. Bind-mount /var/log/clickhouse-server so we can
        //     surface the REAL startup error on failure (ClickHouse writes fatal
        //     errors to clickhouse-server.err.log before crashing, never to stderr). ---
        var logDir = Path.Combine(_tempDir, "logs");
        Directory.CreateDirectory(logDir);

        _container = new ContainerBuilder()
            .WithImage("clickhouse/clickhouse-server:24.3")
            // Grant the default user access-management so dynamic CREATE USER tests
            // (OpenSSH-format / passphrase-encrypted SSH probes) can register
            // throw-away users at runtime instead of pre-baking them via XML.
            .WithEnvironment("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
            .WithPortBinding(NativePort, true)
            .WithPortBinding(SecureNativePort, true)
            .WithBindMount(usersPath, "/etc/clickhouse-server/users.d/auth_test_users.xml", AccessMode.ReadOnly)
            .WithBindMount(tlsConfigPath, "/etc/clickhouse-server/config.d/tls.xml", AccessMode.ReadOnly)
            .WithBindMount(serverCertPath, "/etc/clickhouse-server/certs/server.crt", AccessMode.ReadOnly)
            .WithBindMount(serverKeyPath, "/etc/clickhouse-server/certs/server.key", AccessMode.ReadOnly)
            .WithBindMount(caCertPath, "/etc/clickhouse-server/certs/ca.crt", AccessMode.ReadOnly)
            .WithBindMount(logDir, "/var/log/clickhouse-server", AccessMode.ReadWrite)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilPortIsAvailable(NativePort)
                .UntilPortIsAvailable(SecureNativePort))
            .Build();

        try
        {
            await _container.StartAsync();
        }
        catch (Exception startEx)
        {
            var diagnostics = new StringBuilder();
            try { var (stdout, stderr) = await _container.GetLogsAsync(); diagnostics.Append("STDOUT:\n").Append(stdout).Append("\nSTDERR:\n").Append(stderr); }
            catch (Exception ex) { diagnostics.Append($"<could not fetch docker logs: {ex.Message}>"); }

            var errPath = Path.Combine(logDir, "clickhouse-server.err.log");
            if (File.Exists(errPath))
            {
                try { diagnostics.Append("\n\nclickhouse-server.err.log:\n").Append(File.ReadAllText(errPath)); }
                catch (Exception ex) { diagnostics.Append($"\n<could not read err.log: {ex.Message}>"); }
            }

            throw new InvalidOperationException(
                $"ClickHouse container failed to start.\n{diagnostics}", startEx);
        }
    }

    public async Task DisposeAsync()
    {
        if (_container is not null) await _container.DisposeAsync();
        ClientCertificate?.Dispose();
        CaCertificate?.Dispose();
        if (_tempDir is not null && Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    // OpenSSH public key wire format for RSA: string("ssh-rsa") + mpint(e) + mpint(n)
    // where string = uint32 length + bytes, mpint = uint32 length + two's-complement big-endian bytes.
    private static byte[] EncodeRsaOpenSshPublicKey(RSA rsa)
    {
        var parameters = rsa.ExportParameters(includePrivateParameters: false);
        using var stream = new MemoryStream();
        WriteSshString(stream, Encoding.ASCII.GetBytes("ssh-rsa"));
        WriteSshMpint(stream, parameters.Exponent!);
        WriteSshMpint(stream, parameters.Modulus!);
        return stream.ToArray();
    }

    private static void WriteSshString(Stream stream, byte[] data)
    {
        Span<byte> len = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(len, (uint)data.Length);
        stream.Write(len);
        stream.Write(data);
    }

    private static void WriteSshMpint(Stream stream, byte[] data)
    {
        // Strip leading zero bytes, then prepend a single 0x00 if the top bit is set (to keep sign positive).
        var start = 0;
        while (start < data.Length - 1 && data[start] == 0) start++;
        var effective = data.AsSpan(start);
        var prependZero = effective.Length > 0 && (effective[0] & 0x80) != 0;
        var totalLen = effective.Length + (prependZero ? 1 : 0);

        Span<byte> len = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(len, (uint)totalLen);
        stream.Write(len);
        if (prependZero) stream.WriteByte(0);
        stream.Write(effective);
    }
}

[CollectionDefinition("ClickHouseAuth")]
public class ClickHouseAuthCollection : ICollectionFixture<ClickHouseAuthFixture>
{
}
