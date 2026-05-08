using CH.Native.Connection;

namespace CH.Native.Samples.Hosting;

/// <summary>
/// <c>GET /auth/password</c> — static username + password against the default
/// (un-keyed) <see cref="ClickHouseDataSource"/>. Credentials come from
/// <c>appsettings.json:"ClickHouse"</c> (<c>demo_user</c> / <c>demo</c>) and
/// match the user provisioned by <c>./docker/setup.sh</c>'s
/// <c>initdb/10_auth_and_roles.sql</c>.
/// </summary>
/// <remarks>
/// This is the 80% case — the credentials are static, so no provider is needed.
/// Pass <c>?role=admin_role</c> to activate roles for the request and exercise
/// the grant-gated CREATE/DROP probe.
/// </remarks>
internal static class AuthUserPassword
{
    public static WebApplication MapUserPassword(this WebApplication app)
    {
        app.MapGet("/auth/password",
            (ClickHouseDataSource ds, string? role, CancellationToken ct) =>
                AuthProbe.RunAsync("password", ds, role, ct));
        return app;
    }
}
