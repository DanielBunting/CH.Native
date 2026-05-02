namespace CH.Native.DependencyInjection;

/// <summary>
/// Supplies a password for plain-password authentication. Useful when the
/// password is rotated by an external secret store (Vault, AWS Secrets Manager,
/// Azure Key Vault). Invoked once per physical connection — bounded above by
/// <see cref="Connection.ClickHouseDataSourceOptions.ConnectionLifetime"/>.
/// <para>
/// Failure-driven refresh is not guaranteed: a query failure only re-invokes
/// the provider on the next rent if it caused the pool to discard the
/// connection (e.g. protocol-fatal error, <c>KILL QUERY</c>). Server-side
/// SQL errors leave the connection poolable and reuse the existing password.
/// See the package README ("Invocation cadence") for the full contract.
/// </para>
/// </summary>
public interface IClickHousePasswordProvider
{
    /// <summary>Returns the current password.</summary>
    ValueTask<string> GetPasswordAsync(CancellationToken cancellationToken);
}
