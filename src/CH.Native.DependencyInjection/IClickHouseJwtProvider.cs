namespace CH.Native.DependencyInjection;

/// <summary>
/// Supplies a JWT bearer token for ClickHouse authentication. Invoked by the
/// pool whenever it builds a new physical connection — typically once per
/// <see cref="Connection.ClickHouseDataSourceOptions.ConnectionLifetime"/>
/// window, not per query. Implementations should handle their own caching
/// and refresh behaviour (e.g. via <c>Azure.Identity</c>'s
/// <c>TokenCredential</c> which caches internally).
/// <para>
/// Failure-driven refresh is not guaranteed: a query failure only re-invokes
/// the provider on the next rent if it caused the pool to discard the
/// connection (e.g. protocol-fatal error, <c>KILL QUERY</c>). Server-side
/// SQL errors leave the connection poolable and reuse the existing token.
/// See the package README ("Invocation cadence") for the full contract.
/// </para>
/// </summary>
public interface IClickHouseJwtProvider
{
    /// <summary>Returns a currently-valid JWT for the configured audience.</summary>
    ValueTask<string> GetTokenAsync(CancellationToken cancellationToken);
}
