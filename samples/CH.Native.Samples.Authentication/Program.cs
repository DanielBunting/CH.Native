using System.Security.Cryptography.X509Certificates;
using CH.Native.Connection;
using CH.Native.Exceptions;

if (args.Length == 0 || args[0] is "-h" or "--help")
{
    PrintUsage();
    return 0;
}

var host = Environment.GetEnvironmentVariable("CH_HOST") ?? "localhost";
var port = int.TryParse(Environment.GetEnvironmentVariable("CH_PORT"), out var p) ? p : 9000;
var tlsPort = int.TryParse(Environment.GetEnvironmentVariable("CH_TLS_PORT"), out var tp) ? tp : 9440;

// --role NAME / --role a,b,c may appear anywhere after the subcommand. Pull it
// out first so each builder only sees its own positional args.
var roles = ExtractRolesFlag(ref args);

try
{
    var settings = args[0].ToLowerInvariant() switch
    {
        "password" => BuildPassword(args),
        "jwt"      => BuildJwt(args),
        "ssh"      => BuildSsh(args),
        "cert"     => BuildCert(args),
        _          => throw new ArgumentException($"unknown auth type: {args[0]}")
    };

    await using var connection = new ClickHouseConnection(settings);
    await connection.OpenAsync();

    var user = await connection.ExecuteScalarAsync<string>("SELECT currentUser()");
    var version = await connection.ExecuteScalarAsync<string>("SELECT version()");
    var activeRoles = await connection.ExecuteScalarAsync<string>(
        "SELECT arrayStringConcat(currentRoles(), ',')");

    var rolesDisplay = string.IsNullOrEmpty(activeRoles) ? "(none)" : activeRoles;
    Console.WriteLine($"[{args[0]}] connected as '{user}' roles=[{rolesDisplay}] to ClickHouse {version}");

    // Grant-gated probe: CREATE TABLE needs CREATE + DROP, which admin_role has
    // and analyst (SELECT-only) doesn't. Makes the RBAC contrast concrete for
    // the demo while keeping cleanup idempotent.
    try
    {
        await connection.ExecuteNonQueryAsync(
            "CREATE TABLE IF NOT EXISTS sample_rbac_probe (x UInt8) ENGINE=Memory");
        await connection.ExecuteNonQueryAsync("DROP TABLE IF EXISTS sample_rbac_probe");
        Console.WriteLine($"[{args[0]}]   CREATE/DROP probe = OK (privileged)");
    }
    catch (ClickHouseServerException ex) when (ex.ErrorCode == 497)
    {
        Console.WriteLine($"[{args[0]}]   CREATE/DROP probe = ACCESS_DENIED " +
            "(activate admin_role via --role admin_role to grant this query)");
    }
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"[{args[0]}] failed: {ex.GetType().Name}: {ex.Message}");
    return 1;
}

ClickHouseConnectionSettings BuildPassword(string[] args)
{
    // USAGE: password <user> <password>
    var user = args.ElementAtOrDefault(1) ?? "default";
    var pass = args.ElementAtOrDefault(2) ?? "";
    return ApplyRoles(ClickHouseConnectionSettings.CreateBuilder()
        .WithHost(host).WithPort(port)
        .WithUsername(user)
        .WithPassword(pass))
        .Build();
}

ClickHouseConnectionSettings BuildJwt(string[] args)
{
    // USAGE: jwt <token>
    // NOTE: OSS ClickHouse rejects JWT with "JWT is available only in ClickHouse Cloud".
    // Use against a Cloud endpoint or a build that supports JWT validation.
    var token = args.ElementAtOrDefault(1)
        ?? throw new ArgumentException("jwt: expected <token> argument");
    return ApplyRoles(ClickHouseConnectionSettings.CreateBuilder()
        .WithHost(host).WithPort(port)
        .WithJwt(token))
        .Build();
}

ClickHouseConnectionSettings BuildSsh(string[] args)
{
    // USAGE: ssh <user> <private-key-path> [passphrase]
    // Requires ClickHouse server >= 23.9 (protocol revision >= 54466) and the user
    // configured with an <ssh_keys><ssh_key><base64_key>...</base64_key></ssh_key></ssh_keys>
    // entry in users.xml / users.d/*.xml.
    var user = args.ElementAtOrDefault(1)
        ?? throw new ArgumentException("ssh: expected <user> argument");
    var keyPath = args.ElementAtOrDefault(2)
        ?? throw new ArgumentException("ssh: expected <private-key-path> argument");
    var passphrase = args.ElementAtOrDefault(3);

    return ApplyRoles(ClickHouseConnectionSettings.CreateBuilder()
        .WithHost(host).WithPort(port)
        .WithUsername(user)
        .WithSshKeyPath(keyPath, passphrase))
        .Build();
}

ClickHouseConnectionSettings BuildCert(string[] args)
{
    // USAGE: cert <user> <pfx-path> <pfx-password> [--insecure]
    // Requires the server to be configured with TLS and the user's
    // <ssl_certificates><common_name>...</common_name></ssl_certificates>
    // matching the cert's CN.
    var user = args.ElementAtOrDefault(1)
        ?? throw new ArgumentException("cert: expected <user> argument");
    var pfxPath = args.ElementAtOrDefault(2)
        ?? throw new ArgumentException("cert: expected <pfx-path> argument");
    var pfxPassword = args.ElementAtOrDefault(3)
        ?? throw new ArgumentException("cert: expected <pfx-password> argument");
    var allowInsecure = args.Contains("--insecure");

    var clientCert = new X509Certificate2(pfxPath, pfxPassword);

    var builder = ClickHouseConnectionSettings.CreateBuilder()
        .WithHost(host)
        .WithTls().WithTlsPort(tlsPort)
        .WithUsername(user)
        .WithTlsClientCertificate(clientCert)
        .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate);

    if (allowInsecure) builder.WithAllowInsecureTls();

    return ApplyRoles(builder).Build();
}

ClickHouseConnectionSettingsBuilder ApplyRoles(ClickHouseConnectionSettingsBuilder builder)
    => roles is null ? builder : builder.WithRoles(roles);

static string[]? ExtractRolesFlag(ref string[] args)
{
    // Supported: "--role NAME" or "--role a,b,c" anywhere after the subcommand.
    // Consumes both tokens and returns the remaining args.
    var idx = Array.IndexOf(args, "--role");
    if (idx < 0) return null;
    if (idx + 1 >= args.Length)
        throw new ArgumentException("--role requires a value (role name or comma list).");

    var value = args[idx + 1];
    var roles = value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    args = args.Take(idx).Concat(args.Skip(idx + 2)).ToArray();
    return roles;
}

static void PrintUsage()
{
    Console.WriteLine("""
        CH.Native authentication sample — demonstrates all four auth methods plus
        optional role activation.

        Environment:
          CH_HOST       host (default: localhost)
          CH_PORT       native port (default: 9000)
          CH_TLS_PORT   secure native port (default: 9440)

        Usage:
          dotnet run -- password <user> <password>         [--role NAME | a,b,c]
          dotnet run -- jwt <token>                        [--role NAME | a,b,c]
          dotnet run -- ssh <user> <key-path> [passphrase] [--role NAME | a,b,c]
          dotnet run -- cert <user> <pfx-path> <pfx-pass>  [--insecure] [--role NAME]

        Examples:
          dotnet run -- password demo_user demo
          dotnet run -- password demo_user demo --role analyst
          dotnet run -- ssh  ssh_user  docker/generated/keys/ssh_user --role admin_role
          dotnet run -- cert cert_user docker/generated/certs/client.pfx testpass --insecure --role analyst
          dotnet run -- password demo_user demo --role analyst,admin_role

        Each run opens a connection, runs SELECT currentUser() + currentRoles(),
        and prints both. Without --role the sample's demo users have NO active
        roles (default role is NONE), so queries requiring privileges will fail —
        activating a role demonstrates RBAC in action.
        """);
}
