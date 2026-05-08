using CH.Native.Connection;

namespace CH.Native.Samples.Hosting;

/// <summary>
/// <c>GET /auth/jwt</c> — bearer-token auth against the keyed <c>"primary"</c>
/// <see cref="ClickHouseDataSource"/>. Token is supplied per physical connection
/// by <see cref="Providers.DemoJwtProvider"/>; real apps would plug in
/// Azure.Identity, Auth0, Okta, etc.
/// </summary>
/// <remarks>
/// OSS ClickHouse rejects JWT auth at parse time
/// (<c>"JWT is available only in ClickHouse Cloud"</c>), so this endpoint is
/// expected to surface that error against the docker-compose server. The
/// wiring is here to demonstrate provider shape and pool integration —
/// against a Cloud endpoint the same registration handshakes successfully.
/// </remarks>
internal static class AuthJwt
{
    public static WebApplication MapJwt(this WebApplication app)
    {
        app.MapGet("/auth/jwt",
            ([FromKeyedServices("primary")] ClickHouseDataSource ds, string? role, CancellationToken ct) =>
                AuthProbe.RunAsync("jwt", ds, role, ct));
        return app;
    }
}
