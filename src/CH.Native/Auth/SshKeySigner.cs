using System.Text;
using Renci.SshNet;

namespace CH.Native.Auth;

/// <summary>
/// Signs ClickHouse SSH authentication challenges with an SSH private key.
/// Wraps Renci.SshNet so the SSH dependency is isolated behind one type.
/// </summary>
internal sealed class SshKeySigner : IDisposable
{
    private readonly PrivateKeyFile _privateKey;

    public SshKeySigner(byte[] privateKeyBytes, string? passphrase = null)
    {
        ArgumentNullException.ThrowIfNull(privateKeyBytes);
        if (privateKeyBytes.Length == 0)
            throw new ArgumentException("SSH private key bytes must be non-empty.", nameof(privateKeyBytes));

        using var ms = new MemoryStream(privateKeyBytes, writable: false);
        _privateKey = passphrase is null
            ? new PrivateKeyFile(ms)
            : new PrivateKeyFile(ms, passphrase);
    }

    public SshKeySigner(string privateKeyPath, string? passphrase = null)
    {
        if (string.IsNullOrWhiteSpace(privateKeyPath))
            throw new ArgumentException("Private key path must be non-empty.", nameof(privateKeyPath));

        _privateKey = passphrase is null
            ? new PrivateKeyFile(privateKeyPath)
            : new PrivateKeyFile(privateKeyPath, passphrase);
    }

    /// <summary>
    /// Builds the payload ClickHouse expects: str(protocol_version) + database + user + challenge,
    /// raw byte concatenation with no length prefixes, encoded UTF-8 for the string parts.
    /// </summary>
    internal static byte[] BuildSignedPayload(int protocolVersion, string database, string user, byte[] challenge)
    {
        var ver = Encoding.UTF8.GetBytes(protocolVersion.ToString(System.Globalization.CultureInfo.InvariantCulture));
        var db = Encoding.UTF8.GetBytes(database);
        var userBytes = Encoding.UTF8.GetBytes(user);

        var payload = new byte[ver.Length + db.Length + userBytes.Length + challenge.Length];
        var span = payload.AsSpan();
        ver.CopyTo(span); span = span[ver.Length..];
        db.CopyTo(span); span = span[db.Length..];
        userBytes.CopyTo(span); span = span[userBytes.Length..];
        challenge.CopyTo(span);
        return payload;
    }

    /// <summary>
    /// Signs the payload and returns the SSH-wire-format signature blob
    /// (length-prefixed algorithm name + length-prefixed signature bytes).
    /// </summary>
    public byte[] Sign(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);

        var algorithm = _privateKey.HostKeyAlgorithms.FirstOrDefault()
            ?? throw new InvalidOperationException("SSH private key exposes no host-key algorithms.");

        return algorithm.Sign(payload);
    }

    public void Dispose() => _privateKey.Dispose();
}
