using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using Xunit;

namespace CH.Native.Tests.Fixtures;

/// <summary>
/// Shared test fixture that manages a ClickHouse container with TLS enabled for integration tests.
/// </summary>
public class ClickHouseTlsFixture : IAsyncLifetime
{
    private const string TestUsername = "default";
    private const string TestPassword = "test_password";
    private const int SecureNativePort = 9440;

    private IContainer? _container;
    private string? _tempDir;

    /// <summary>
    /// Gets the hostname of the ClickHouse container.
    /// </summary>
    public string Host => _container?.Hostname ?? throw new InvalidOperationException("Container not started");

    /// <summary>
    /// Gets the secure native protocol port (9440) mapped to the host.
    /// </summary>
    public int TlsPort => _container?.GetMappedPublicPort(SecureNativePort) ?? throw new InvalidOperationException("Container not started");

    /// <summary>
    /// Gets the username for authentication.
    /// </summary>
    public string Username => TestUsername;

    /// <summary>
    /// Gets the password for authentication.
    /// </summary>
    public string Password => TestPassword;

    /// <summary>
    /// Gets a connection string for the secure native protocol.
    /// </summary>
    public string ConnectionString => $"Host={Host};TlsPort={TlsPort};UseTls=true;AllowInsecureTls=true;Username={Username};Password={Password}";

    public async Task InitializeAsync()
    {
        // Create temp directory for certificates and config
        _tempDir = Path.Combine(Path.GetTempPath(), $"ch_tls_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);

        // Generate self-signed certificate
        var (certPath, keyPath) = GenerateSelfSignedCertificate(_tempDir);

        // Create ClickHouse config for TLS
        var configPath = CreateTlsConfig(_tempDir);

        _container = new ContainerBuilder()
            .WithImage("clickhouse/clickhouse-server:24.1")
            .WithEnvironment("CLICKHOUSE_USER", TestUsername)
            .WithEnvironment("CLICKHOUSE_PASSWORD", TestPassword)
            .WithPortBinding(SecureNativePort, true)
            .WithBindMount(certPath, "/etc/clickhouse-server/server.crt", AccessMode.ReadOnly)
            .WithBindMount(keyPath, "/etc/clickhouse-server/server.key", AccessMode.ReadOnly)
            .WithBindMount(configPath, "/etc/clickhouse-server/config.d/ssl.xml", AccessMode.ReadOnly)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilPortIsAvailable(SecureNativePort))
            .Build();

        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        if (_container != null)
        {
            await _container.DisposeAsync();
        }

        // Clean up temp directory
        if (_tempDir != null && Directory.Exists(_tempDir))
        {
            try
            {
                Directory.Delete(_tempDir, recursive: true);
            }
            catch
            {
                // Best effort cleanup
            }
        }
    }

    private static (string certPath, string keyPath) GenerateSelfSignedCertificate(string outputDir)
    {
        using var rsa = RSA.Create(2048);

        var request = new CertificateRequest(
            "CN=localhost",
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);

        // Add extensions
        request.CertificateExtensions.Add(
            new X509BasicConstraintsExtension(false, false, 0, true));

        request.CertificateExtensions.Add(
            new X509KeyUsageExtension(
                X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment,
                true));

        // Add Subject Alternative Names for localhost
        var sanBuilder = new SubjectAlternativeNameBuilder();
        sanBuilder.AddDnsName("localhost");
        sanBuilder.AddIpAddress(System.Net.IPAddress.Loopback);
        sanBuilder.AddIpAddress(System.Net.IPAddress.IPv6Loopback);
        request.CertificateExtensions.Add(sanBuilder.Build());

        // Create self-signed certificate
        var certificate = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(1));

        // Export certificate (PEM format)
        var certPath = Path.Combine(outputDir, "server.crt");
        var certPem = certificate.ExportCertificatePem();
        File.WriteAllText(certPath, certPem);

        // Export private key (PEM format)
        var keyPath = Path.Combine(outputDir, "server.key");
        var keyPem = rsa.ExportRSAPrivateKeyPem();
        File.WriteAllText(keyPath, keyPem);

        return (certPath, keyPath);
    }

    private static string CreateTlsConfig(string outputDir)
    {
        var configPath = Path.Combine(outputDir, "ssl.xml");
        var configContent = """
            <clickhouse>
                <tcp_port_secure>9440</tcp_port_secure>
                <openSSL>
                    <server>
                        <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
                        <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
                        <verificationMode>none</verificationMode>
                        <loadDefaultCAFile>false</loadDefaultCAFile>
                        <cacheSessions>true</cacheSessions>
                        <disableProtocols>sslv2,sslv3</disableProtocols>
                        <preferServerCiphers>true</preferServerCiphers>
                    </server>
                </openSSL>
            </clickhouse>
            """;

        File.WriteAllText(configPath, configContent);
        return configPath;
    }
}

/// <summary>
/// Collection definition for tests that share a single TLS-enabled ClickHouse container.
/// </summary>
[CollectionDefinition("ClickHouseTls")]
public class ClickHouseTlsCollection : ICollectionFixture<ClickHouseTlsFixture>
{
}
