namespace CH.Native.Samples.Hosting;

/// <summary>
/// Coordinator for the four <c>/auth/{method}</c> probe endpoints. Each
/// per-method file (<see cref="AuthUserPassword"/>, <see cref="AuthJwt"/>,
/// <see cref="AuthSshKey"/>, <see cref="AuthClientCertificate"/>) owns its
/// own route registration and explanatory commentary; this file just stitches
/// them together so <c>Program.cs</c> stays tidy.
/// </summary>
internal static class AuthEndpoints
{
    public static WebApplication MapAuthEndpoints(this WebApplication app) => app
        .MapUserPassword()
        .MapJwt()
        .MapSshKey()
        .MapClientCertificate();
}
