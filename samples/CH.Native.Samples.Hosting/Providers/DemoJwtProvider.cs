using CH.Native.DependencyInjection;

namespace CH.Native.Samples.Hosting.Providers;

/// <summary>
/// Stub <see cref="IClickHouseJwtProvider"/>. Real apps would call into
/// Azure.Identity's TokenCredential, Auth0, Okta, etc. The wiring shape is the
/// same — the provider returns a fresh bearer token per physical connection.
/// </summary>
public sealed class DemoJwtProvider : IClickHouseJwtProvider
{
    public ValueTask<string> GetTokenAsync(CancellationToken cancellationToken)
        => ValueTask.FromResult("demo.jwt.token");
}
