namespace CH.Native.DependencyInjection;

/// <summary>
/// Supplies a password for plain-password authentication. Useful when the
/// password is rotated by an external secret store (Vault, AWS Secrets Manager,
/// Azure Key Vault). Invoked once per physical connection.
/// </summary>
public interface IClickHousePasswordProvider
{
    /// <summary>Returns the current password.</summary>
    ValueTask<string> GetPasswordAsync(CancellationToken cancellationToken);
}
