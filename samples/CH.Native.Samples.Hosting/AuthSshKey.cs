using CH.Native.Connection;

namespace CH.Native.Samples.Hosting;

/// <summary>
/// <c>GET /auth/ssh</c> — SSH-key auth against the keyed <c>"ssh"</c>
/// <see cref="ClickHouseDataSource"/>. Key material is supplied per physical
/// connection by <see cref="Providers.DemoSshKeyProvider"/>, which reads the
/// RSA private key written under <c>docker/generated/keys/ssh_user</c> by
/// <c>./docker/setup.sh</c>.
/// </summary>
/// <remarks>
/// Requires ClickHouse server >= 23.9 (protocol revision >= 54466) and a user
/// configured with an
/// <c>&lt;ssh_keys&gt;&lt;ssh_key&gt;&lt;base64_key&gt;...&lt;/base64_key&gt;&lt;/ssh_key&gt;&lt;/ssh_keys&gt;</c>
/// entry — both of which the docker overlay's <c>setup.sh</c> + <c>initdb</c>
/// SQL provision automatically.
/// </remarks>
internal static class AuthSshKey
{
    public static WebApplication MapSshKey(this WebApplication app)
    {
        app.MapGet("/auth/ssh",
            ([FromKeyedServices("ssh")] ClickHouseDataSource ds, string? role, CancellationToken ct) =>
                AuthProbe.RunAsync("ssh", ds, role, ct));
        return app;
    }
}
