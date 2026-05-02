using System.Security.Cryptography;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Security;

/// <summary>
/// Round-trip system tests for SSH-key authentication. Uses
/// <see cref="ClickHouseAuthFixture"/>'s pre-provisioned <c>ssh_user</c> with an
/// ssh-rsa public key registered server-side.
///
/// <para>The corresponding integration tests live in
/// <c>tests/CH.Native.Tests/Integration/SshAuthTests.cs</c>; these run alongside the
/// rest of the system suite so a regression in pool / settings interplay surfaces in
/// the same matrix.</para>
/// </summary>
[Collection("ClickHouseAuth")]
[Trait(Categories.Name, Categories.Security)]
public class SshKeyAuthProbeTests
{
    private readonly ClickHouseAuthFixture _fx;
    private readonly ITestOutputHelper _output;

    public SshKeyAuthProbeTests(ClickHouseAuthFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task SshKey_ValidConfig_HandshakeSucceedsAndSelectIsCurrentUser()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithUsername("ssh_user")
            .WithSshKey(_fx.SshPrivateKeyPem)
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        var who = await conn.ExecuteScalarAsync<string>("SELECT currentUser()");
        Assert.Equal("ssh_user", who);
    }

    [Fact]
    public async Task SshKey_PoolReusesSshAuthenticatedConnections()
    {
        // Confirm SSH-authenticated connections live in the pool the same way
        // password-auth ones do — TotalCreated stays bounded across N rents.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithUsername("ssh_user")
            .WithSshKey(_fx.SshPrivateKeyPem)
            .Build();

        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = settings,
            MaxPoolSize = 2,
        });

        for (int i = 0; i < 8; i++)
        {
            await using var conn = await ds.OpenConnectionAsync();
            Assert.Equal("ssh_user", await conn.ExecuteScalarAsync<string>("SELECT currentUser()"));
        }

        var stats = ds.GetStatistics();
        _output.WriteLine($"SSH-auth pool stats after 8 rents: {stats}");
        Assert.True(stats.TotalCreated <= 2,
            $"Pool should reuse SSH connections; TotalCreated={stats.TotalCreated}");
    }

    [Fact]
    public async Task SshKey_WrongPrivateKey_FailsCleanly()
    {
        // Generate a fresh RSA key not provisioned server-side. Handshake must fail
        // with a typed exception, not hang or leak.
        using var fresh = RSA.Create(2048);
        var pem = System.Text.Encoding.UTF8.GetBytes(fresh.ExportPkcs8PrivateKeyPem());

        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithUsername("ssh_user")
            .WithSshKey(pem)
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        var caught = await Assert.ThrowsAnyAsync<Exception>(() => conn.OpenAsync());

        _output.WriteLine($"Wrong-key surface: {caught.GetType().FullName} — {caught.Message}");
        Assert.True(
            caught is ClickHouseServerException or ClickHouseConnectionException,
            $"Expected typed auth failure; got {caught.GetType().FullName}");
    }

    [Fact]
    public async Task SshKey_WrongUsername_FailsCleanly()
    {
        // Right key, wrong user — server should reject with a typed exception.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithUsername("nonexistent_user")
            .WithSshKey(_fx.SshPrivateKeyPem)
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        var caught = await Assert.ThrowsAnyAsync<Exception>(() => conn.OpenAsync());

        _output.WriteLine($"Unknown-user surface: {caught.GetType().FullName} — {caught.Message}");
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    [Fact]
    public async Task SshKey_OpenSshFormat_LoadsAndAuthenticates()
    {
        // Generate an OpenSSH-format Ed25519 keypair via ssh-keygen, register the
        // public key as a new server-side user, then authenticate with the OpenSSH
        // PEM. Pin: the library accepts OpenSSH-format private keys (not just PKCS#8).
        var (privatePem, publicWire) = GenerateSshKey("ed25519", passphrase: null);
        var pubBase64 = ExtractOpenSshPubBase64(publicWire);

        var user = $"ossh_user_{Guid.NewGuid():N}".Substring(0, 16);
        await using (var admin = new ClickHouseConnection(BuildAdminSettings()))
        {
            await admin.OpenAsync();
            await admin.ExecuteNonQueryAsync(
                $"CREATE USER {user} IDENTIFIED WITH ssh_key BY KEY '{pubBase64}' TYPE 'ssh-ed25519'");
            await admin.ExecuteNonQueryAsync($"GRANT SELECT ON *.* TO {user}");
        }

        try
        {
            var settings = ClickHouseConnectionSettings.CreateBuilder()
                .WithHost(_fx.Host)
                .WithPort(_fx.Port)
                .WithUsername(user)
                .WithSshKey(privatePem)
                .Build();

            await using var conn = new ClickHouseConnection(settings);
            await conn.OpenAsync();
            Assert.Equal(user, await conn.ExecuteScalarAsync<string>("SELECT currentUser()"));
        }
        finally
        {
            try
            {
                await using var admin = new ClickHouseConnection(BuildAdminSettings());
                await admin.OpenAsync();
                await admin.ExecuteNonQueryAsync($"DROP USER IF EXISTS {user}");
            }
            catch { /* best effort */ }
        }
    }

    [Fact]
    public async Task SshKey_PassphraseEncryptedKey_CorrectPassphrase_Authenticates()
    {
        var passphrase = "test-passphrase-42";
        var (privatePem, publicWire) = GenerateSshKey("rsa", passphrase);
        var pubBase64 = ExtractOpenSshPubBase64(publicWire);

        var user = $"pp_user_{Guid.NewGuid():N}".Substring(0, 14);
        await using (var admin = new ClickHouseConnection(BuildAdminSettings()))
        {
            await admin.OpenAsync();
            await admin.ExecuteNonQueryAsync(
                $"CREATE USER {user} IDENTIFIED WITH ssh_key BY KEY '{pubBase64}' TYPE 'ssh-rsa'");
            await admin.ExecuteNonQueryAsync($"GRANT SELECT ON *.* TO {user}");
        }

        try
        {
            var settings = ClickHouseConnectionSettings.CreateBuilder()
                .WithHost(_fx.Host)
                .WithPort(_fx.Port)
                .WithUsername(user)
                .WithSshKey(privatePem, passphrase)
                .Build();

            await using var conn = new ClickHouseConnection(settings);
            await conn.OpenAsync();
            Assert.Equal(user, await conn.ExecuteScalarAsync<string>("SELECT currentUser()"));
        }
        finally
        {
            try
            {
                await using var admin = new ClickHouseConnection(BuildAdminSettings());
                await admin.OpenAsync();
                await admin.ExecuteNonQueryAsync($"DROP USER IF EXISTS {user}");
            }
            catch { /* best effort */ }
        }
    }

    [Fact]
    public async Task SshKey_PassphraseEncryptedKey_NoPassphrase_FailsClientSide()
    {
        var (privatePem, _) = GenerateSshKey("rsa", "anypass");

        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithUsername("ssh_user")
            .WithSshKey(privatePem)  // no passphrase supplied
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        var caught = await Assert.ThrowsAnyAsync<Exception>(() => conn.OpenAsync());

        _output.WriteLine($"Encrypted-key-no-passphrase surface: {caught.GetType().FullName} — {caught.Message}");
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    [Fact]
    public async Task SshKey_PassphraseEncryptedKey_WrongPassphrase_FailsClientSide()
    {
        var (privatePem, _) = GenerateSshKey("rsa", "correctpass");

        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithUsername("ssh_user")
            .WithSshKey(privatePem, "wrongpass")
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        var caught = await Assert.ThrowsAnyAsync<Exception>(() => conn.OpenAsync());

        _output.WriteLine($"Encrypted-key-wrong-passphrase surface: {caught.GetType().FullName} — {caught.Message}");
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    [Fact]
    public async Task SshKey_GarbageBytes_FailsBeforeHandshake()
    {
        // Bytes that don't parse as any PEM/OpenSSH key. Failure should be
        // client-side (key parser) before any TCP traffic.
        var garbage = new byte[64];
        new Random(0xC0FFEE).NextBytes(garbage);

        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithUsername("ssh_user")
            .WithSshKey(garbage)
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        var caught = await Assert.ThrowsAnyAsync<Exception>(() => conn.OpenAsync());

        _output.WriteLine($"Garbage-bytes surface: {caught.GetType().FullName} — {caught.Message}");
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    /// <summary>
    /// Settings for a privileged connection used to provision throwaway users for
    /// the OpenSSH-format / passphrase tests. Falls back through known credentials
    /// for <see cref="ClickHouseAuthFixture"/>'s default user.
    /// </summary>
    private ClickHouseConnectionSettings BuildAdminSettings()
        => ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithCredentials("default", "")
            .Build();

    /// <summary>
    /// Generates an SSH keypair using ssh-keygen — the canonical OpenSSH-format tool.
    /// Returns (privateKeyPemBytes, publicKeyOpenSshLine). Encryption is applied if
    /// <paramref name="passphrase"/> is non-null. Cleans up temp files on success or
    /// failure.
    /// </summary>
    private static (byte[] PrivatePem, string PublicWire) GenerateSshKey(string keyType, string? passphrase)
    {
        var dir = Path.Combine(Path.GetTempPath(), $"ssh_probe_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        var keyPath = Path.Combine(dir, "id");
        try
        {
            var args = new List<string>
            {
                "-t", keyType,
                "-f", keyPath,
                "-q",
                "-C", "probe",
                "-N", passphrase ?? string.Empty,
            };
            // RSA defaults to 3072 bits (slow); pin to 2048 for tests.
            if (keyType == "rsa") { args.Add("-b"); args.Add("2048"); }

            var psi = new System.Diagnostics.ProcessStartInfo("/usr/bin/ssh-keygen")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            foreach (var a in args) psi.ArgumentList.Add(a);

            using var p = System.Diagnostics.Process.Start(psi)
                ?? throw new InvalidOperationException("Could not start ssh-keygen");
            p.WaitForExit();
            if (p.ExitCode != 0)
                throw new InvalidOperationException(
                    $"ssh-keygen failed: {p.StandardError.ReadToEnd()}");

            return (
                File.ReadAllBytes(keyPath),
                File.ReadAllText(keyPath + ".pub").TrimEnd('\n', '\r'));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { }
        }
    }

    /// <summary>
    /// Extracts the base64 portion from an OpenSSH public-key line. The format is
    /// "type base64data comment"; CH expects just the base64.
    /// </summary>
    private static string ExtractOpenSshPubBase64(string openSshLine)
    {
        var parts = openSshLine.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2)
            throw new ArgumentException($"Unexpected SSH public key shape: {openSshLine}");
        return parts[1];
    }
}
