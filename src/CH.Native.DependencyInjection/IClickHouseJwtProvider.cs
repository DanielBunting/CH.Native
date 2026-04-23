namespace CH.Native.DependencyInjection;

/// <summary>
/// Supplies a JWT bearer token for ClickHouse authentication. Invoked by the
/// pool whenever it builds a new physical connection — typically once per
/// <see cref="Connection.ClickHouseDataSourceOptions.ConnectionLifetime"/>
/// window, not per query. Implementations should handle their own caching
/// and refresh behaviour (e.g. via <c>Azure.Identity</c>'s
/// <c>TokenCredential</c> which caches internally).
/// </summary>
public interface IClickHouseJwtProvider
{
    /// <summary>Returns a currently-valid JWT for the configured audience.</summary>
    ValueTask<string> GetTokenAsync(CancellationToken cancellationToken);
}
