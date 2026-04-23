namespace CH.Native.DependencyInjection;

/// <summary>
/// Private key material plus optional passphrase, returned by
/// <see cref="IClickHouseSshKeyProvider"/>.
/// </summary>
public sealed record SshKeyMaterial(byte[] PrivateKey, string? Passphrase = null);

/// <summary>
/// Supplies an SSH private key for ClickHouse SSH-key authentication.
/// Invoked once per physical connection.
/// </summary>
public interface IClickHouseSshKeyProvider
{
    /// <summary>Returns the SSH private key to sign the server challenge with.</summary>
    ValueTask<SshKeyMaterial> GetKeyAsync(CancellationToken cancellationToken);
}
